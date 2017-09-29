import unittest
import logging
# logging.basicConfig(level=logging.DEBUG)
import time

from scale_client.core.broker import Broker
from seismic_alert_server import SeismicAlertServer
from seismic_alert_subscriber import SeismicAlertSubscriber
from seismic_alert_common import *

from scale_client.sensors.dummy.dummy_virtual_sensor import DummyVirtualSensor


class TestSeismicWarning(unittest.TestCase):
    """Tests the various features of the seismic warning test code.  It's mostly for the purposes of ensuring the
    event aggregation mechanisms work properly, but there's also some tests of various utility functions here."""

    def setUp(self):
        broker = Broker()
        self.sub = SeismicAlertSubscriber(broker=broker, remote_brokers=['localhost'])
        # Just like in actual tests, need to have events use a sequence # as the data to distinguish different quakes!
        self.pub = DummyVirtualSensor(broker=broker, name='SeismicPublisher', event_type="seismic", dynamic_event_data=dict(seq=0))
        self.srv = SeismicAlertServer(broker=broker)

    def test_empty(self):
        """Verify that a pick with an empty (null-value) payload is ignored, but NOT one with 0-value!."""
        # Test with payload being None
        ev = self.pub.make_event_with_raw_data(None)
        self.srv.on_event(ev, ev.topic)
        self.srv.read()
        self.assertFalse(self.srv.events_rcvd, "server should have ignored null-payload event!")

        # do more copies
        for i in range(5):
            ev = self.pub.make_event_with_raw_data(None)
            self.srv.on_event(ev, ev.topic)
        self.srv.read()
        self.assertFalse(self.srv.events_rcvd, "server should have ignored null-payload event!")

        ev = self.pub.make_event_with_raw_data(0.0)
        self.srv.on_event(ev, ev.topic)
        self.assertFalse(self.srv.events_to_process.empty())
        self.srv.read()
        self.assertTrue(self.srv.events_rcvd, "server should not have ignored 0-value payload event!")
        self.assertTrue(self.srv.events_to_process.empty())

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

        # First collect a bunch of duplicated events for one unique 'seismic event'.  Some share the same source.
        n_events = 5
        n_sources = 3
        events = self._generate_events(1, n_sources, n_duplicates=n_events)
        # TODO: probably need to do this generation again, do events.extend(dup_events), and re-work these tests since
        # new changes to our code will break these tests that assume only the source matters...

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

        # XXX NOTE: we changed the seismic scenario data model to only include the event ID in order to save space in
        # the Coap packet; hence, a lot of these checks aren't done anymore since those fields aren't present.

        # Sleep to ensure we have different enough timestamps...
        SLEEP_TIME = 0.1
        # they should be within this delta of each other
        delta = 0.01

        create_time = time.time()
        # Gather up events, some of which are from the same source
        n_quakes = 3
        n_pubs = 2
        n_dups = 5
        events = self._generate_events(n_quakes, n_pubs, n_duplicates=5)

        # Simulate a delay from when the events are uploaded to server and when the server aggregates them.
        time.sleep(SLEEP_TIME)
        for e in events:
            self.srv.on_event(e, e.topic)
        aggd_time = time.time()
        agg_ev = self.srv.read()

        # Simulate delay from reporting aggregate alert to when the subscriber receives it.
        time.sleep(SLEEP_TIME)
        rcv_time = time.time()
        self.sub.on_event(agg_ev, agg_ev.topic)

        # Now we verify our expected results:
        self.assertGreater(len(self.sub.events_rcvd), 1, "subscriber should have received more than 1 event!")
        for stats in self.sub.events_rcvd.values():
            # self.assertAlmostEqual(stats['time_sent'], create_time, delta=delta)
            self.assertAlmostEqual(stats['time_rcvd'], rcv_time, delta=delta)
            # self.assertAlmostEqual(stats['time_aggd'], aggd_time, delta=delta)
        self.assertAlmostEqual(agg_ev.timestamp, aggd_time, delta=delta)

        # make more events, push through to sub, and verify it keeps the time FIRST received
        events = self._generate_events(n_quakes, n_pubs, n_dups)
        for ev in events:
            self.srv.on_event(ev, ev.topic)
        ev = self.srv.read()
        self.sub.on_event(ev, ev.topic)
        for stats in self.sub.events_rcvd.values():
            # self.assertAlmostEqual(stats['time_sent'], create_time, delta=delta)
            self.assertAlmostEqual(stats['time_rcvd'], rcv_time, delta=delta)
            # self.assertAlmostEqual(stats['time_aggd'], aggd_time, delta=delta)

    def test_multiple_events(self):
        """Test that the aggregator and subscriber properly distinguish different seismic events based on the sequence
        number in event.data"""

        # Produce a bunch of events with some duplicates and verify that the expected number of unique ones is correct.
        n_quakes = 3
        n_pubs = 4
        n_dups = 5
        events = self._generate_events(n_quakes, n_pubs, n_dups)

        # Now feed them to the server in order and get the aggregate a couple times along the way to verify
        # that aggregation is working properly, including the subscriber's duplicate counts.
        for i in range(n_dups):
            for j in range(n_quakes):
                for k in range(n_pubs):
                    ev = events.pop(0)
                    self.srv.on_event(ev, ev.topic)

                # Verify server aggregation
                agg = self.srv.read()
                # after first iteration of outer loop, we won't be adding any new events!
                n_expected_events = n_pubs * ((j+1) if i == 0 else n_quakes)
                actual_n_events = len(agg.data)
                self.assertEqual(actual_n_events, n_expected_events,
                                 "should have %d events in this aggregate alert after quake #%d"
                                 " but we have %d!" % (n_expected_events, j, actual_n_events))

                # verify subscriber
                self.sub.on_event(agg, agg.topic)
                self.assertEqual(len(self.sub.events_rcvd), n_expected_events)

                # ENHANCE: could validate #copies by e.g. extracting seq # and verifying it, but the most recent events
                # will be only 1 copy whereas the first events will have i+1 copies.

    # Test utility functions

    def test_event_id(self):
        event1_src1 = self.pub.make_event_with_raw_data(5)
        event1_src2 = self.pub.make_event_with_raw_data(5)
        event1_src2.source = "other_sensor"
        event2_src1 = self.pub.make_event_with_raw_data(0)
        event2_src2 = self.pub.make_event_with_raw_data(0)
        event2_src2.source = "other_sensor"

        self.assertEqual(get_event_id(event1_src2), 'other_sensor/5')
        self.assertNotEqual(get_event_id(event1_src2), get_event_id(event1_src1))
        self.assertNotEqual(get_event_id(event1_src2), get_event_id(event2_src2))
        self.assertNotEqual(get_event_id(event2_src1), get_event_id(event2_src2))

    def test_event_id_serialization(self):
        """Test the ability to pack/unpack event IDs into/from a more compressed binary form in order to save space."""

        # PLAN: just pack then unpack the events' IDs to ensure we can transfer them in a compressed format on the wire
        events = self._generate_events(3, 3)

        # to make sure they aren't all equivalent somehow, track which ones we've seen
        ev_ids_seen = set()
        for ev in events:
            ev_id = get_event_id(ev)
            comp_ev_id = pack_event_id(ev_id)
            uncompressed_ev_id = unpack_event_id(comp_ev_id)

            self.assertEqual(ev_id, uncompressed_ev_id)
            self.assertEqual(len(comp_ev_id), PACKED_EVENT_ID_LEN)
            self.assertNotIn(uncompressed_ev_id, ev_ids_seen)

            ev_ids_seen.add(uncompressed_ev_id)

        self.assertEqual(len(ev_ids_seen), 9)

    def test_seismic_alert_serialization(self):
        """Test the ability to pack/unpack the seismic alert data (list of event IDs) into/from a more compressed
         binary form in order to save space."""

        # PLAN: just pack then unpack the events' IDs to ensure we can transfer them in a compressed format on the wire
        events = self._generate_events(3, 3)

        # NOW, we need to make sure we can properly (de)compress the whole list of event IDs
        ev_ids = [get_event_id(e) for e in events]
        comp_ev_ids = pack_seismic_alert_data(ev_ids)
        uncompressed_ev_ids = unpack_seismic_alert_data(comp_ev_ids)

        self.assertEqual(ev_ids, uncompressed_ev_ids)

    # Helper functions used across multiple tests

    def _generate_events(self, n_unique_events, n_sources, n_duplicates=1):
        """
        Generate the given number of events for each of the given number of sources.  Also generates a number of
        duplicate events for the requested number.
        """

        # TODO: if we change the APIs and need a more portable method of generating duplicate events, could create
        # multiple publisher instances and have each of them generate events, copying these events for each duplicate.

        events = []
        for k in range(n_duplicates):
            for i in range(n_unique_events):
                for j in range(n_sources):
                    ev = self.pub.make_event_with_raw_data(i)
                    ev.source = "coap://10.0.0.%d/sensors/DummySensor" % j
                    events.append(ev)

        return events

if __name__ == '__main__':
    unittest.main()
