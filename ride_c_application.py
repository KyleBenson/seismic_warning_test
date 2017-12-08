# @author: Kyle Benson
# (c) Kyle Benson 2017

from ride.ride_c import RideC
from ride.data_path_monitor import RideCDataPathMonitor

from scale_client.core.threaded_application import ThreadedApplication
from seismic_alert_common import DATA_PATH_UPDATE_TOPIC, PUBLISHER_ROUTE_TOPIC

import threading
import logging
log = logging.getLogger(__name__)


class RideCApplication(RideC, ThreadedApplication):
    """
    This Application runs on an edge server to interact with an SDN Controller for the purposes of ensuring more
    resilient IoT data collection.  It does this by monitoring the DataPaths to the cloud (through local gateways)
    and redirecting IoT data flows to the best available one whenever a disruption is detected.  If none are available,
    it will redirect all these data flows to an edge server in order to maintain operations despite failures/congestion.
    """

    def __init__(self, broker,
                 # RideC parameters
                 # ENHANCE: replace these with an API...
                 publishers=tuple(), data_paths=tuple(), dst_port=9999,
                 subscriptions=(DATA_PATH_UPDATE_TOPIC,),
                 maintenance_interval=10, **kwargs):
        """
        See also the parameters for RideC constructor!

        :param broker:
        :param publishers: collection of tuples containing the parameters (ip_addr, src_port) needed for registering all
         publisher hosts (NOTE that this will eventually move to an API!!)
        :param data_paths: collection of 4-tuples representing the (data_path_id, gateway_dpid, cloud_server_dpid, probe_src_port)
        parameters used to register each DataPath
        :param dst_port: port of remote echo server that probes will be sent to
        :param maintenance_interval: seconds between running topology updates and reconstructing publisher routes
         if necessary, accounting for topology changes
        :param kwargs:
        """
        # XXX: need to specify broker as a kwarg so it doesn't get passed to RideC as a positional
        super(RideCApplication, self).__init__(broker=broker, subscriptions=subscriptions,
                                               n_threads=len(data_paths),  # need a thread for each DataPathMonitor
                                               advertisements=[PUBLISHER_ROUTE_TOPIC, DATA_PATH_UPDATE_TOPIC], **kwargs)

        self.maintenance_interval = maintenance_interval

        # ENHANCE: remove these when we move it to an API...
        self.publishers = publishers

        # ENHANCE: re-enable this when we migrate to an API...
        # If we need to do anything with the server right away or expect some logic to be called
        # that will not directly check whether the server is running currently, we should wait
        # for a CoapServerRunning event before accessing the actual server.
        # NOTE: make sure to do this here, not on_start, as we currently only send the ready notification once!
        # ev = CoapServer.CoapServerRunning(None)
        # self.subscribe(ev, callback=self.__class__.__on_coap_ready)

        # Register Datapaths first so that when we register the publishers they'll be assigned a DataPath automatically
        self._data_path_monitors = []
        for dp, gw, cloud, src_port in data_paths:
            self.register_data_path(dp, gw, cloud)
            # these will be started later, but we can create the objects for now
            dpm = RideCDataPathMonitor(data_path_id=dp, address=self.topology_manager.get_ip_address(cloud),
                                       dst_port=dst_port, src_port=src_port, status_change_callback=self.__dp_status_change_cb)
            self._data_path_monitors.append(dpm)

        # XXX: need to prevent multiple status changes from occurring simultaneously
        self.__dp_status_lock = threading.Lock()

    def __dp_status_change_cb(self, data_path_id, link_status):
        """Since the DPMonitors are running in background threads, we need to have them fire a callback that publishes
        an event rather than calling self.on_data_path_status_change directly: this not only allows other apps to
        receive such updates, but it also keeps us from having occasional threading-induced errors when e.g. both DPs
        go DOWN simultaneously."""
        update_event = self.make_event(data=dict(data_path_id=data_path_id, status=link_status), event_type=DATA_PATH_UPDATE_TOPIC)
        self.publish(update_event, topic=DATA_PATH_UPDATE_TOPIC)

    def _on_all_data_paths_down(self):
        """
        We need to publish updates about the publishers being re-routed to the edge if all DataPaths are down. Hence,
        we just override this method here to add this functionality.
        :return:
        """

        super(RideCApplication, self)._on_all_data_paths_down()
        # update ALL the routes since they've been all re-directed
        self.publish_route_updates(self._host_routes)

    def __maintain_topology(self):
        """Runs periodically to check for topology updates, reconstruct the MDMTs if necessary, and update flow
        rules to account for these topology changes or newly-joined/leaving subscribers."""

        # TODO: should periodically run this?  not in our simulation experiments currently though...
        # THREADING: may need to lock data structures during this so we don't e.g. establish a route that no longer exists
        updated_routes = self.update()
        self.publish_route_updates(updated_routes)

    def publish_route_updates(self, updated_routes):
        """Helper function to locally publish 'publisher_route_update' events, mainly for the benefit of RideD"""
        # XXX: since RideD only accepts IP addresses, we need to extract that from the host addresses that may include ports
        # ENHANCE: either move this conversion to RideD, or accept a complete host address in RideD
        updated_routes = {self._get_host_ip_address(k): v for k,v in updated_routes.items()}
        route_update_event = self.make_event(data=updated_routes, event_type=PUBLISHER_ROUTE_TOPIC)
        self.publish(route_update_event, topic=PUBLISHER_ROUTE_TOPIC)

    def on_start(self):
        """
        Register the DataPaths and publishers requested, setting their flow-based routes along the way.
        """
        super(RideCApplication, self).on_start()

        # This will choose a DataPath for them and install its flow rules.
        # We also need to publish these updated DataPaths' routes.
        routes_assigned = dict()
        for pub in self.publishers:
            # XXX: pub format needs to be a tuple in order to be hashed into a dict
            pub = tuple(pub)
            self.register_host(pub)
            route = self._host_routes[pub]
            routes_assigned[pub] = route

        self.publish_route_updates(routes_assigned)

        # Start DataPath monitors for each registered DataPath
        # NOTE: we do this after the publisher routes in case a DP-monitor alerts us to a down DP, which could get
        # overwritten by the subsequent publisher route assignment
        for dpm in self._data_path_monitors:
            assert isinstance(dpm, RideCDataPathMonitor)  # type hinting
            self.run_in_background(dpm.run)
            # TESTING: can enable this hacky test that pretends it's a DP-monitor detecting a down DP
            # self.timed_call(t, self.__class__.on_data_path_status_change, False, dp, DATA_PATH_DOWN)

    def on_stop(self):
        """Close each running DataPathMonitor"""
        for dpm in self._data_path_monitors:
            dpm.finish()
        super(RideCApplication, self).on_stop()

    def on_event(self, event, topic):
        """Whenever we receive a DataPath update event, pass its contents to RideC to update the status."""
        assert topic == DATA_PATH_UPDATE_TOPIC, "received non-DataPath update event with topic %s" % topic

        # XXX: we seem to get errors when multiple data paths fail simultaneously: set changed size, list index out of
        # range, etc., which all seem to be related to circuits processing events simultaneously?
        with self.__dp_status_lock:
            self.on_data_path_status_change(**event.data)

    # ENHANCE: migrate publisher/data_path registration to an API rather than command line...
    #
    # def __on_coap_ready(self, server):
    #     """
    #     Register a CoAP API endpoint for ???????????????????????????????????????????????????????????????
    #     :param CoapServer server:
    #     :return:
    #     """
    #
    #     # ENHANCE: could save server name to make sure we've got the right one her?
    #     # if self._server_name is None or self._server_name == server.name:
    #     self._server = server
    #
    #     def __process_coap_subscription(coap_request, coap_resource):
    #         """
    #         Extract the relevant information from the CoAP request object and pass it along to our handler
    #         :param coap_request:
    #         :type coap_request: coapthon.messages.request.Request
    #         :param coap_resource:
    #         :return:
    #         """
    #         host, port = coap_request.source
    #         payload = coap_request.payload
    #
    #         if True: #self.process_subscription(topic, host):
    #             return coap_resource
    #         else:
    #             return False
    #
    #     # ENHANCE: how to handle an unsubscribe?
    #     path = SUBSCRIPTION_API_PATH
    #
    #     # server.register_api(path, name="%s subscription registration" % SEISMIC_ALERT_TOPIC,
    #     #                     post_callback=__process_coap_subscription, allow_children=True)
