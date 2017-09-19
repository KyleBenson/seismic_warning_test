# @author: Kyle Benson
# (c) Kyle Benson 2017


from ride.ride_c import RideC

from scale_client.core.threaded_application import ThreadedApplication
from seismic_alert_common import DATA_PATH_UPDATE_TOPIC, PUBLISHER_ROUTE_TOPIC

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
                 # TODO: replace these with an API...
                 publishers=tuple(), data_paths=tuple(),
                 subscriptions=(DATA_PATH_UPDATE_TOPIC,),
                 maintenance_interval=10, **kwargs):
        """
        See also the parameters for RideC constructor!

        :param broker:
        :param maintenance_interval: seconds between running topology updates and reconstructing publisher routes
         if necessary, accounting for topology changes
        :param kwargs:
        """
        # XXX: need to specify broker as a kwarg so it doesn't get passed to RideC as a positional
        super(RideCApplication, self).__init__(broker=broker, subscriptions=subscriptions,
                                               advertisements=[PUBLISHER_ROUTE_TOPIC], **kwargs)

        self.maintenance_interval = maintenance_interval

        # TODO: remove these when we move it to an API...
        self.publishers = publishers

        # ENHANCE: re-enable this when we migrate to an API...
        # If we need to do anything with the server right away or expect some logic to be called
        # that will not directly check whether the server is running currently, we should wait
        # for a CoapServerRunning event before accessing the actual server.
        # NOTE: make sure to do this here, not on_start, as we currently only send the ready notification once!
        # ev = CoapServer.CoapServerRunning(None)
        # self.subscribe(ev, callback=self.__class__.__on_coap_ready)

        # Register Datapaths first so that when we register the publishers they'll be assigned a DataPath automatically
        for dp, gw, cloud in data_paths:
            self.register_data_path(dp, gw, cloud)
        # TODO: we might be responsible for spinning up the pinger processes for these data_paths...

    def __maintain_topology(self):
        """Runs periodically to check for topology updates, reconstruct the MDMTs if necessary, and update flow
        rules to account for these topology changes or newly-joined/leaving subscribers."""

        # TODO: may need to lock data structures during this so we don't e.g. establish a route that no longer exists
        update_routes = self.update()

        route_update_event = self.make_event(data=update_routes, topic=PUBLISHER_ROUTE_TOPIC)
        self.publish(route_update_event, topic=PUBLISHER_ROUTE_TOPIC)

    def on_start(self):
        """
        Register the DataPaths and publishers requested, setting their flow-based routes along the way.
        """
        super(RideCApplication, self).on_start()

        # TODO: start pingers

        # This will choose a DataPath for them and install its flow rules.
        # We also need to publish these updated DataPaths' routes.
        routes_assigned = dict()
        for pub in self.publishers:
            self.register_host(pub)
            route = self._host_routes[pub]
            routes_assigned[pub] = route

        route_update_event = self.make_event(data=routes_assigned, topic=PUBLISHER_ROUTE_TOPIC)
        self.publish(route_update_event, topic=PUBLISHER_ROUTE_TOPIC)

    def on_event(self, event, topic):
        """Whenever we receive a DataPath update event, pass its contents to RideC to update the status."""
        assert topic == DATA_PATH_UPDATE_TOPIC, "received non-DataPath update event with topic %s" % topic

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

    # TODO: close pingers?
    # def on_stop(self):
    #     """Close any open network connections"""
    #     self.coap_client.close()
    #     super(RideCApplication, self).on_stop()
