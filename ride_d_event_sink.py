# @author: Kyle Benson
# (c) Kyle Benson 2017

import logging
from threading import Lock

log = logging.getLogger(__name__)

from ride.ride_d import RideD, nx

from scale_client.event_sinks.event_sink import ThreadedEventSink
from scale_client.networks.coap_server import CoapServer
from scale_client.networks.coap_client import CoapClient
from scale_client.networks.util import DEFAULT_COAP_PORT, coap_response_success, coap_code_to_name, CoapCodes
from seismic_alert_common import *

# We typically wait until on_start to actually build RideD since it takes a while due to
# creating/updating the topology_manager, but you can selectively have it start right away.
# This is mostly for type hinting to an IDE that self.rided can be of type RideD
BUILD_RIDED_IN_INIT = False

class RideDEventSink(ThreadedEventSink):
    """
    An EventSink that delivers events using the RIDE-D middleware for resilient IP multicast-based publishing.
    """

    def __init__(self, broker,
                 # RideD parameters
                 # TODO: sublcass RideD in order to avoid code repetition here for extracting parameters?
                 dpid, addresses=None, topology_mgr='onos', ntrees=2,
                 tree_choosing_heuristic='importance', tree_construction_algorithm=('red-blue',),
                 # XXX: rather than running a separate service that would intercept incoming publications matching the
                 # specified flow for use in the STT, we simply wait for seismic picks and use them as if they're
                 # incoming packets.  This ignores other potential packets from those hosts, but this will have to do
                 # for now since running such a separated service would require more systems programming than this...
                 subscriptions=(SEISMIC_PICK_TOPIC,
                                # RideD gathers the publisher routes from RideC via events when they change
                                PUBLISHER_ROUTE_TOPIC,
                                ),
                 maintenance_interval=10,
                 multicast=True, port=DEFAULT_COAP_PORT, topics_to_sink=(SEISMIC_ALERT_TOPIC,), **kwargs):
        """
        See also the parameters for RideD constructor!
        :param ntrees: # MDMTs to build (passed to RideD constructor); note that setting this to 0 disables multicast!

        :param broker:
        :param address_pool: iterable of IP addresses (formatted as strings) that can be used to register multicast trees
        :param port: port number to send events to (NOTE: we expect all subscribers to listen on the same port OR
        for you to configure the flow rules to convert this port to the expected one before delivery to subscriber)
        :param topics_to_sink: a SensedEvent whose topic matches one in this list will be
        resiliently multicast delivered; others will be ignored
        :param maintenance_interval: seconds between running topology updates and reconstructing MDMTs if necessary,
        accounting for topology changes or new/removed subscribers
        :param multicast: if True (default unless ntrees==0), build RideD for using multicast; otherwise, subscribers are alerting one
        at a time (async) using unicast
        :param kwargs:
        """
        super(RideDEventSink, self).__init__(broker, topics_to_sink=topics_to_sink, subscriptions=subscriptions, **kwargs)

        # Catalogue active subscribers' host addresses (indexed by topic with value being a set of subscribers)
        self.subscribers = dict()

        self.port = port
        self.maintenance_interval = maintenance_interval

        # If we need to do anything with the server right away or expect some logic to be called
        # that will not directly check whether the server is running currently, we should wait
        # for a CoapServerRunning event before accessing the actual server.
        # NOTE: make sure to do this here, not on_start, as we currently only send the ready notification once!
        ev = CoapServer.CoapServerRunning(None)
        self.subscribe(ev, callback=self.__class__.__on_coap_ready)

        # Store parameters for RideD resilient multicast middleware; we'll actually build it later since it takes a while...
        self.use_multicast = multicast if ntrees else False

        if self.use_multicast and addresses is None:
            raise NotImplementedError("you must specify the multicast 'addresses' parameter if multicast is enabled!")

        if not self.use_multicast:
            self.rided = None
        elif not BUILD_RIDED_IN_INIT:
            self.rided = dict(topology_mgr=topology_mgr, dpid=dpid, addresses=addresses, ntrees=ntrees,
                              tree_choosing_heuristic=tree_choosing_heuristic, tree_construction_algorithm=tree_construction_algorithm)
        else:
            self.rided = RideD(topology_mgr=topology_mgr, dpid=dpid, addresses=addresses, ntrees=ntrees,
                               tree_choosing_heuristic=tree_choosing_heuristic, tree_construction_algorithm=tree_construction_algorithm)

        # Use a single client to connect with each server
        # COAPTHON-SPECIFIC: unclear that we'd be able to do this in all future versions...
        # NOTE: we specify a dummy server_hostname because we'll explicitly set it each time we use the client,
        # but it has to be a valid one to avoid causing an error...
        srv_ip = '10.0.0.1'
        if addresses:
            srv_ip = addresses[0]
        self.coap_client = CoapClient(server_hostname=srv_ip, server_port=self.port, confirmable_messages=not self.use_multicast)

        # Use thread locks to prevent simultaneous write access to data structures due to e.g.
        # handling multiple simultaneous subscription registrations.
        self.__subscriber_lock = Lock()


    def __maintain_topology(self):
        """Runs periodically to check for topology updates, reconstruct the MDMTs if necessary, and update flow
        rules to account for these topology changes or newly-joined/leaving subscribers."""

        # ENHANCE: only update the necessary changes: old subscribers are easy to trim, new ones could be added directly,
        # and topologies could be compared for differences (though that's probably about the same work as just refreshing the whole thing)
        # TODO: probably need to lock rided during this so we don't e.g. send_event to an MDMT that's currently being reconfigured.... maybe that's okay though?
        self.rided.update()

    def on_start(self):
        """
        Build and configure the RideD middleware
        """
        # TODO: probably run this in the background?

        if self.rided is not None:
            if not BUILD_RIDED_IN_INIT:
                assert isinstance(self.rided, dict)
                self.rided = RideD(**self.rided)
            assert isinstance(self.rided, RideD)

            # Rather than periodically update the topology, which in our experiments would result in perfectly routing
            # around all the failures due to 0-latency control plane, we just update it once for now...
            self.timed_call(self.maintenance_interval, self.__class__.__maintain_topology, repeat=False)
            # self.timed_call(self.maintenance_interval, self.__class__.__maintain_topology, repeat=True)

        super(RideDEventSink, self).on_start()

    def __sendto(self, msg, topic, address, port=None, callback=None):
        """
        Sends msg to the specified address using CoAP.  topic is used to define the path of the CoAP
        resource we PUT the msg in.
        :param msg:
        :param topic:
        :param address:
        :param port:
        :param callback: called upon receiving a response the this message (default=self.__put_event_callback)
        :return:
        """
        if port is None:
            port = self.port
        if callback is None:
            callback = self.__put_event_callback

        # TODO: don't hardcode this...
        path = "/events/%s" % topic

        # By setting the 'server' attribute, we're telling the client what destination to use.
        # ENHANCE: COAPTHON-SPECIFIC: should probably make some @properties to keep these in line
        self.coap_client.server = (address, port)

        # Use async mode to send this message as otherwise sending a bunch of them can lead to a back log...
        self.coap_client.put(path=path, payload=msg, callback=callback)

        log.debug("RIDE-D message sent: topic=%s ; address=%s ; payload_length=%d" % (topic, address, len(msg)))

    def __put_event_callback(self, response, mdmt_used=None):
        """
        This callback handles the CoAP response for a PUT message.  In addition to logging the success or failure it
        notifies RideD of the response's route (using the provided mdmt_used parameter) if configured for
        reliable multicast delivery.
        :param response:
        :type response: coapthon.messages.response.Response
        :param mdmt_used: if specified, the request was sent via reliable multicast and this parameter represents the
        multicast tree used
        :type mdmt_used: nx.Graph
        :return:
        """

        # TODO: record results to output later?

        # XXX: when client closes the last response is a NoneType
        if response is None:
            return
        elif coap_response_success(response):
            log.debug("successfully sent alert to " + str(response.source))

            if mdmt_used:
                # determine the path used by this response and notify RideD that it is currently functional
                responder_ip_addr = response.source[0]
                responder = self.rided.topology_manager.get_host_by_ip(responder_ip_addr)
                route = nx.shortest_path(mdmt_used, responder, self.rided.get_server_id())
                # XXX: just directly update the STT... perhaps we should have an API in RideD for this in the future?
                self.rided.stt_mgr.route_update(route)
                log.debug("updating STT with response route: %s" % route)

        elif response.code == CoapCodes.NOT_FOUND.number:
            log.warning("remote rejected PUT request for uncreated object: did you forget to add that resource?")
        else:
            log.error("failed to send aggregated events due to Coap error: %s" % coap_code_to_name(response.code))

    def send_event(self, event):
        """
        When charged with sending an event, we will send it to each subscriber.  If configured for using multicast,
        we first choose the best MDMT for resilient multicast delivery."""

        topic = event.topic
        encoded_event = self.encode_event(event)
        log.debug("Sending event via RIDE-D with topic %s" % topic)

        # Send the event as we're configured to
        try:
            # Determine the best MDMT, get the destination associated with it, and send the event.
            if self.use_multicast:
                # if we ever encounter this, replace it with some real error handling...
                assert self.rided is not None, "woops!  Ride-D should be set up but it isn't..."

                try:
                    mdmt = self.rided.get_best_mdmt(topic)
                except KeyError:
                    log.error("currently-unhandled error likely caused by trying to MDMT-multicast"
                              " an alert to an unregistered topic with no MDMTs!")
                    return False

                address = self.rided.get_address_for_mdmt(mdmt)
                log.debug("using best available MDMT with address %s" % address)

                # The callback needs to know which MDMT was used so that it can determine the response's route and
                # update the STT accordingly.
                def __mdmt_response_callback(response):
                    self.__put_event_callback(response, mdmt)
                self.__sendto(encoded_event, topic=topic, address=address, callback=__mdmt_response_callback)

            # Configured as unicast, so send a message to each subscriber individually
            else:
                for address in self.subscribers.get(topic, []):
                    self.__sendto(encoded_event, topic=topic, address=address)

            return True

        except IOError as e:
            log.error("failed to send event via CoAP PUT due to error: %s" % e)
            return False

    def on_event(self, event, topic):
        """
        We receive sensor-publisher route updates via events from RideC.
        HACK: any seismic picks we receive are treated as incoming publications for the purposes of updating the
        STT.  This clearly does not belong in a finalized version of the RideD middleware, which would instead
        intercept actual packets matching a particular flow and use them to update the STT.
        :param event:
        :type event: scale_client.core.sensed_event.SensedEvent
        :param topic:
        :return:
        """

        if topic == SEISMIC_PICK_TOPIC:

            if self.rided and not event.is_local:
                # Find the publishing host's IP address and use that to notify RideD
                publisher = event.source
                # ENHANCE: accept full address (e.g. ipv4_add, port) as publisher IDs just like RideC!
                publisher = get_hostname_from_path(publisher)
                assert publisher is not None, "error processing publication with no source hostname: %s" % event.source
                # TODO: may need to wrap this with mutex
                self.rided.notify_publication(publisher, id_type='ip')

        elif topic == PUBLISHER_ROUTE_TOPIC:

            if self.rided:
                for host, route in event.data.items():
                    log.debug("setting publisher route from event: host(%s) --> %s" % (host, route))
                    self.rided.set_publisher_route(host, route)

        else:
            assert False, "received non-seismic event we didn't subscribe to! topic=%s" % topic

    def process_subscription(self, topic, host):
        """
        Handles a subscription request by adding the host to the current subscribers.
        Note that we don't collect a port number or protocol type as we currently assume it will be
        CoAP and its well-known port number.
        :param topic:
        :param host: IP address or hostname of subscribing host (likely taken from CoAP request)
        :return:
        """

        log.debug("processing RIDE-D subscription for topic '%s' by host '%s'" % (topic, host))
        with self.__subscriber_lock:
            self.subscribers.setdefault(topic, set()).add(host)

        if self.rided:
            # WARNING: supposedly we should only register subscribers that are reachable in our topology view or
            #  we'll cause errors later... we should try to handle those errors instead!
            try:
                # ENHANCE: handle port numbers? all ports will be same for our scenario and OF could convert them anyway so no hurry...
                host = self.rided.topology_manager.get_host_by_ip(host)
                # If we can't find a path, how did we even get this subscription?  Path failed after it was sent?
                self.rided.topology_manager.get_path(host, self.rided.dpid)
                with self.__subscriber_lock:
                    self.rided.add_subscriber(host, topic_id=SEISMIC_ALERT_TOPIC)
            except BaseException as e:
                log.warning("Route between subscriber %s and server %s not found: skipping...\nError: %s" % (host, self.rided.dpid, e))
                return False

        return True

    def __on_coap_ready(self, server):
        """
        Register a CoAP API endpoint for subscribers to register their subscriptions through.
        :param CoapServer server:
        :return:
        """

        if self.use_multicast:
            # TODO: if we ever encounter this, we should delay registering the subscriptions API until after ride-d is setup
            # maybe we could just defer the arriving subscription by not sending a response?
            assert self.rided is not None, "woops coap is set up but ride-d isn't!!"

        # ENHANCE: could save server name to make sure we've got the right one her?
        # if self._server_name is None or self._server_name == server.name:
        self._server = server

        def __process_coap_subscription(coap_request, coap_resource):
            """
            Extract the relevant subscription information from the CoAP request object and pass it along to self.process_subscription()
            :param coap_request:
            :type coap_request: coapthon.messages.request.Request
            :param coap_resource:
            :return:
            """
            host, port = coap_request.source
            payload = coap_request.payload
            # ENHANCE: check the content-type?
            topic = payload
            # TODO: remove this hack later
            assert topic == SEISMIC_ALERT_TOPIC, "unrecognized subscription topic %s" % topic

            if self.process_subscription(topic, host):
                return coap_resource
            else:
                return False

        # ENHANCE: how to handle an unsubscribe?
        path = SUBSCRIPTION_API_PATH

        server.register_api(path, name="%s subscription registration" % SEISMIC_ALERT_TOPIC,
                            post_callback=__process_coap_subscription, allow_children=True)

    def check_available(self, event):
        """We only deliver events whose topic matches those that have been registered
         with RIDE-D and currently have subscribers."""
        return super(RideDEventSink, self).check_available(event) and event.topic in self.subscribers

    def on_stop(self):
        """Close any open network connections e.g. CoapClient"""
        self.coap_client.close()
        super(RideDEventSink, self).on_stop()

        # TODO: log error when no subscribers ever connected?

    def encode_event(self, event):
        """Encodes the given event with several fields stripped out and only the most recent event IDs in order to save
        space in the single CoAP packet it will be sunk in."""
        return compress_alert_one_coap_packet(event)