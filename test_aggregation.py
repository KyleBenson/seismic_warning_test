import unittest
import logging
# logging.basicConfig(level=logging.DEBUG)
import time

from scale_client.core.broker import Broker
from seismic_alert_server import SeismicAlertServer
from seismic_alert_subscriber import SeismicAlertSubscriber
from seismic_alert_common import *

from scale_client.sensors.dummy.heartbeat_sensor import HeartbeatSensor


class TestAggregation(unittest.TestCase):

    def setUp(self):
        broker = Broker()
        self.sub = SeismicAlertSubscriber(broker=broker)
        self.pub = HeartbeatSensor(broker=broker, event_type=SEISMIC_PICK_TOPIC)
        self.srv = SeismicAlertServer(broker=broker)

    def test_empty(self):
        """Verify that a pick with an empty (or 0-value) payload is ignored."""
        ev = self.pub.make_event_with_raw_data(0.0)
        self.srv.on_event(ev, ev.topic)
        self.assertFalse(self.srv.events_to_process.empty())
        self.srv.read()
        self.assertFalse(self.srv.events_rcvd, "server should have ignored 0-value payload event!")
        self.assertTrue(self.srv.events_to_process.empty())

        # Test with payload being None
        ev = self.pub.make_event_with_raw_data(None)
        self.srv.on_event(ev, ev.topic)
        self.srv.read()
        self.assertFalse(self.srv.events_rcvd, "server should have ignored null-payload event!")

        # do more copies
        for i in range(5):
            ev = self.pub.make_event_with_raw_data(0)
            self.srv.on_event(ev, ev.topic)
        self.srv.read()
        self.assertFalse(self.srv.events_rcvd, "server should have ignored null-payload event!")

    def test_simple_aggregation(self):
        """Verifies that the server will aggregate one event at a time, building up the collection of received events.
        The subscriber should behave similarly."""

        self.assertEqual(len(self.sub.events_rcvd), 0)

        ev = self.pub.make_event_with_raw_data(1.0)
        self.srv.on_event(ev, ev.topic)
        agg = self.srv.read()
        self.assertEqual(len(agg.data), 1, "should only have a single event in this aggregate alert!")

        self.sub.on_event(agg, agg.topic)
        self.assertEqual(len(self.sub.events_rcvd), 1)

        ev = self.pub.make_event_with_raw_data(1.0)
        ev.source = self.sub.path
        self.srv.on_event(ev, ev.topic)
        agg = self.srv.read()
        self.assertEqual(len(agg.data), 2, "should have 2 events in this aggregate alert!")

        self.sub.on_event(agg, agg.topic)
        self.assertEqual(len(self.sub.events_rcvd), 2)

        ev = self.pub.make_event_with_raw_data(1.0)
        ev.source = "nonsense"
        self.srv.on_event(ev, ev.topic)
        agg = self.srv.read()
        self.assertEqual(len(agg.data), 3, "should have 3 events in this aggregate alert!")

        self.sub.on_event(agg, agg.topic)
        self.assertEqual(len(self.sub.events_rcvd), 3)

    def test_multi_aggregation(self):
        """Verifies that the server will properly aggregate multiple events that have piled up.
        Similarly, the subscriber should be able to handle larger aggregate alerts."""

        n_events = 5
        for i in range(n_events):
            ev = self.pub.make_event_with_raw_data(i+1)
            ev.source = "sensor%d" % i
            self.srv.on_event(ev, ev.topic)

        agg = self.srv.read()
        self.assertEqual(len(agg.data), n_events, "should have %d events in this aggregate alert!" % n_events)
        self.assertGreater(len(agg.data), 0)

        # verify subscriber
        self.sub.on_event(agg, agg.topic)
        self.assertEqual(len(self.sub.events_rcvd), n_events)

    def test_duplicate_aggregation(self):
        """Verifies that we properly aggregate duplicates by counting the # of occurrences but keep the original."""

        # First collect a bunch of events, some of which share the same source.
        n_events = 5
        n_sources = 3
        events = []
        for i in range(n_events):
            for j in range(n_sources):
                ev = self.pub.make_event_with_raw_data(i+1)
                ev.source = "sensor%d" % j
                events.append(ev)

        # Now feed them to the server in order and get the aggregate a couple times along the way to verify
        # that aggregation is working properly, including the subscriber's duplicate counts.
        for i in range(n_events):
            for j in range(n_sources):
                ev = events.pop(0)
                self.srv.on_event(ev, ev.topic)
            agg = self.srv.read()
            self.assertEqual(len(agg.data), n_sources, "should have %d events in this aggregate alert after every iteration here!" % n_sources)

            # verify subscriber
            self.sub.on_event(agg, agg.topic)
            for stats in self.sub.events_rcvd.values():
                self.assertEqual(stats['copies_rcvd'], i+1)
            self.assertEqual(len(self.sub.events_rcvd), n_sources)

    def test_timestamps(self):
        """Verify that the timestamps of when the original picks were created get carried through;
        also ensure that the alert's time is the time it was aggregated.  Also check that the subscriber
        records the time FIRST received."""

        # Sleep to ensure we have different enough timestamps...
        SLEEP_TIME = 0.1
        # they should be within this delta of each other
        delta = 0.001

        create_time = time.time()
        events = []
        # Gather up events, some of which are from the same source
        for i in range(3):
            ev = self.pub.make_event_with_raw_data(i + 1)
            events.append(ev)
            ev = self.pub.make_event_with_raw_data(2*i + 1)
            ev.source = "sensor%d" % i
            events.append(ev)

        time.sleep(SLEEP_TIME)
        aggd_time = time.time()
        for e in events:
            self.srv.on_event(e, e.topic)
        agg_ev = self.srv.read()

        time.sleep(SLEEP_TIME)
        rcv_time = time.time()
        self.sub.on_event(agg_ev, agg_ev.topic)

        self.assertGreater(len(self.sub.events_rcvd), 1)
        for stats in self.sub.events_rcvd.values():
            self.assertAlmostEqual(stats['time_sent'], create_time, delta=delta)
            self.assertAlmostEqual(stats['time_rcvd'], rcv_time, delta=delta)
        self.assertAlmostEqual(agg_ev.timestamp, aggd_time, delta=delta)

        # make more events, push through to sub, and verify it keeps the time FIRST received
        ev = self.pub.make_event_with_raw_data(7)
        self.srv.on_event(ev, ev.topic)
        time.sleep(SLEEP_TIME)
        ev = self.pub.make_event_with_raw_data(7)
        self.srv.on_event(ev, ev.topic)
        ev = self.srv.read()
        self.sub.on_event(ev, ev.topic)
        for stats in self.sub.events_rcvd.values():
            self.assertAlmostEqual(stats['time_sent'], create_time, delta=delta)
            self.assertAlmostEqual(stats['time_rcvd'], rcv_time, delta=delta)


if __name__ == '__main__':
    unittest.main()
