# @author: Kyle Benson
# (c) Kyle Benson 2017
import json
import logging
log = logging.getLogger(__name__)

import time
from Queue import Queue

from scale_client.networks.coap_server import CoapServer
from scale_client.sensors.virtual_sensor import VirtualSensor
from seismic_alert_common import *


class SeismicAlertServer(VirtualSensor):
    """
    Simple seismic event detection and alerting module for the SCALE Client.
    It processes 'seismic' SensedEvents from clients (called 'picks' in CSN terms) that
    represent significant background shaking indicative of a possible earthquake.
    It aggregates together all of these readings over a short period of time and
    then forwards the combined data ('seismic_alert') directly to interested subscribers.
    This forwarding is done using CoAP over UDP and the RideD resilient multicast middleware.
    """

    def __init__(self, broker, sample_interval=2, event_type=SEISMIC_ALERT_TOPIC,
                 output_events_file="output_events_rcvd",
                 subscriptions=(SEISMIC_PICK_TOPIC,), **kwargs):
        super(SeismicAlertServer, self).__init__(broker, event_type=event_type,
                                                 sample_interval=sample_interval, subscriptions=subscriptions, **kwargs)

        # We store events in a thread-safe queue until they can be properly aggregated every 'sample_interval' seconds
        self.events_to_process = Queue()

        # Stores received events indexed by their 'id'
        self.events_rcvd = dict()

        # Need to know when a CoapServer is running so we can open an endpoint for receiving seismic events.
        ev = CoapServer.CoapServerRunning(None)
        self.subscribe(ev, callback=self.__class__.__on_coap_ready)

        # Store all events received in their original form to output to file for audit/testing purposes
        self.output_file = output_events_file
        self.__output_events = []

    def __on_coap_ready(self, server):
        """
        Once the CoapServer is ready, we need to open an endpoint for receiving seismic events.
        :return:
        """
        # TODO: store server name and check we get the right one?
        # ENHANCE: maybe this is a common pattern for scale modules that use coap resources?  really it's a remote_coap_subscribe(topic, cb=None)???  maybe this belongs in a RemotePubSubManager class to handle all these things...
        event = self.make_event(event_type=SEISMIC_PICK_TOPIC, data=None)
        # TODO: not hard-code this
        path = '/events/%s' % SEISMIC_PICK_TOPIC
        # NOTE: no one remote should POST/DEL only PUT
        server.store_event(event, path, disable_post=True, disable_delete=True)

    def read_raw(self):
        """After receiving the first 'pick', a 'seismic_alert' SensedEvent is created
        every 'sample_interval' seconds.  This alert contains aggregated relevant data for all of
        the individual picks received during this earthquake (currently we never flush the event
        buffer so this means it aggregates ALL picks it ever received since being activated)."""

        # No events yet!
        if self.events_to_process.empty() and not self.events_rcvd:
            return None

        # Receive and process all the new events by storing the ones we haven't received
        # TODO: should we keep track of how many duplicates get aggregated?  the original seismic_server didn't...
        # Furthermore, the only time we receive duplicates should be if an ACK is lost since each successive event
        # published should have a different sequence #.

        while not self.events_to_process.empty():
            ev = self.events_to_process.get()
            log.debug("processing event %s" % ev)
            ev_id = get_event_id(ev)
            # Skip over any null-payload events entirely, store all others for outputting to file, and otherwise only
            # keep events with new IDs not seen before for the aggregation mechanism.
            if ev.data is not None:
                # TODO: maybe we shouldn't be skipping over ones we've already processed?  nothing to do with them currently though...
                if ev_id not in self.events_rcvd:
                    self.events_rcvd[ev_id] = ev
                self.__output_events.append(ev)

        # Then aggregate them and return the result for publication
        # ENHANCE: cache this and add new arrivals to it for better efficiency?
        agg_events = {ev_id: dict(time_sent=ev.timestamp, time_aggd=ev.metadata['time_aggd']) for ev_id, ev in self.events_rcvd.items()}

        return agg_events

    def on_event(self, event, topic):
        """Store this event for later aggregation"""
        if topic is None:
            topic = event.topic
        assert topic == SEISMIC_PICK_TOPIC

        log.debug("received seismic event for later processing")

        event.metadata['time_aggd'] = time.time()
        self.events_to_process.put(event)

    def policy_check(self, event):
        return event is not None and event.data is not None

    def on_stop(self):
        """Records the received picks for consumption by another script
        that will analyze the resulting performance."""
        super(SeismicAlertServer, self).on_stop()

        with open(self.output_file, "w") as f:
            f.write(json.dumps([e.to_map() for e in self.__output_events]))
