import unittest
import logging
# logging.basicConfig(level=logging.DEBUG)
import time

from scale_client.core.broker import Broker
from scale_client.core.sensed_event import SensedEvent
from seismic_alert_server import SeismicAlertServer
from seismic_alert_subscriber import SeismicAlertSubscriber
from seismic_alert_common import *

from scale_client.sensors.dummy.dummy_virtual_sensor import DummyVirtualSensor


class TestSeismicWarning(unittest.TestCase):

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

        # Because we only publish the event after receiving new events to aggregate, we use this flag to check that.
        once_through = False

        # Now feed them to the server in order and get the aggregate a couple times along the way to verify
        # that aggregation is working properly, including the subscriber's duplicate counts.
        for i in range(n_events):
            for j in range(n_sources):
                ev = events.pop(0)
                self.srv.on_event(ev, ev.topic)
            agg = self.srv.read()
            if not once_through:
                self.assertEqual(len(agg.data), n_sources, "should have %d events in this aggregate alert after every iteration here!" % n_sources)

                # verify subscriber only for actual aggregated events
                self.sub.on_event(agg, agg.topic)
                for stats in self.sub.events_rcvd.values():
                    self.assertEqual(stats['copies_rcvd'], i+1)
                self.assertEqual(len(self.sub.events_rcvd), n_sources)

                once_through = True
            else:
                self.assertIsNone(agg.data, "aggd event data should be null after first round through since all remaining rounds are duplciates!")

    def test_timestamps(self):
        """Verify that the timestamps of when the original picks were created get carried through;
        also ensure that the alert's time is the time it was aggregated.  Also check that the subscriber
        records the time FIRST received."""

        # XXX NOTE: we changed the seismic scenario data model to only include the event ID in order to save space in
        # the Coap packet; hence, a lot of these checks aren't done anymore since those fields aren't present.

        # Sleep to ensure we have different enough timestamps...
        SLEEP_TIME = 0.1
        # they should be within this delta of each other
        # NOTE: we've had to increase this from 0.001, so you might find that on a different system the tests fail.  As
        # long as they're within a reasonable delta of each other (could increase SLEEP_TIME too) this should be
        # considered a successful test.
        delta = 0.005

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
        time.sleep(SLEEP_TIME)
        events = self._generate_events(n_quakes, n_pubs, n_dups)
        time.sleep(SLEEP_TIME)
        for ev in events:
            self.srv.on_event(ev, ev.topic)

        # XXX: because we've already seen these events, the aggregated event will have null data if we don't add something new!
        events = self._generate_events(1, 1, 1, sensor_name='new_sensor%d')
        for ev in events:
            self.srv.on_event(ev, ev.topic)

        ev = self.srv.read()
        time.sleep(SLEEP_TIME)
        self.sub.on_event(ev, ev.topic)
        print self.sub.events_rcvd
        for source, stats in self.sub.events_rcvd.items():
            if 'new_sensor' in source:
                continue
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

        # Once we go through once, all duplicates should return events with no data since nothing new to aggregate
        once_through = False

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
                if not once_through:
                    n_expected_events = n_pubs * ((j+1) if i == 0 else n_quakes)
                    actual_n_events = len(agg.data)
                    self.assertEqual(actual_n_events, n_expected_events,
                                     "should have %d events in this aggregate alert after quake #%d"
                                     " but we have %d!" % (n_expected_events, j, actual_n_events))

                    # verify subscriber
                    self.sub.on_event(agg, agg.topic)
                    self.assertEqual(len(self.sub.events_rcvd), n_expected_events)
                else:
                    self.assertIsNone(agg.data, "after first round we only have duplicates so the event should have null data!")

                # ENHANCE: could validate #copies by e.g. extracting seq # and verifying it, but the most recent events
                # will be only 1 copy whereas the first events will have i+1 copies.

            once_through = True

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

    def test_alert_compression(self, n_sensors=20, n_quakes=10, expect_one_packet=False):
        """Tests the method for compressing the data in a seismic alert event so that it fits in a single CoAP packet.
        Note that this test doesn't actually verify that the packet isn't fragmented on the wire, but rather relies on
        the CoAP helper function to check if it's too big."""

        # IDEA: create pick events, pass them to server, get its aggregated alert event, compress that, and then verify
        # that it fits into a single CoAP packet and actually contains recent events

        events = self._generate_events(n_quakes, n_sensors, event_type=SEISMIC_PICK_TOPIC, time_incr=1)
        # NOTE: because of the varying event ID lengths and our attempt to quickly cut out some that are over capacity,
        # we add some more events with longer name lengths to verify that part works.  You can enable logging and add
        # print statements to the function to manually verify this, though automated tests with more than 2 different
        # event ID lengths would be much better.  WARNING: this test wasn't catching a bug due to this fact!
        big_event_id = 'big_huge_accelerometer_thing%d'
        events.extend(self._generate_events(n_quakes, n_sensors / 4, event_type=SEISMIC_PICK_TOPIC, sensor_name=big_event_id))
        for e in events:
            self.srv.on_event(e, topic=e.topic)

        alert = self.srv.read()
        if not expect_one_packet:
            self.assertFalse(msg_fits_one_coap_packet(alert.to_json()), "alert msg already fits into one packet! add more pick events...")

        comp_alert = compress_alert_one_coap_packet(alert)
        self.assertTrue(msg_fits_one_coap_packet(comp_alert), "compressed alert data doesn't actually fit in one packet!")

        # double-check the contained events are newer ones
        decomp_alert = SensedEvent.from_json(comp_alert)
        # print "alert data was:", alert.data, "\nBut now is:", decomp_alert.data
        # print "seq #s after compression are:", [get_seq_from_event_id(eid) for eid in decomp_alert.data]
        self.assertIsInstance(decomp_alert.data, list)  # make sure it isn't just a single event string...
        self.assertTrue(any(get_seq_from_event_id(eid) == n_quakes - 1 for eid in decomp_alert.data))  # should keep newest

        # Don't run these checks for the tests that verify it works ok with only a few events
        if not expect_one_packet:
            self.assertGreaterEqual(len(decomp_alert.data), 5)  # where all the events at??
            self.assertTrue(all(get_seq_from_event_id(eid) > 0 for eid in decomp_alert.data))  # should throw out oldest
            # check to make sure we're using most of the packet.  Note that we assume here nquakes < 1000
            self.assertGreater(len(comp_alert), COAP_MAX_PAYLOAD_SIZE - (len(big_event_id) + 4))

    # Using the above test, now we verify some edge conditions and provides opportunity to verify other behaviors.

    def test_alert_compression_not_full(self):
        # Verify we don't cause errors when there's plenty of room in the packet
        self.test_alert_compression(n_sensors=3, expect_one_packet=True)

    def test_alert_compression_single_event(self):
        # Verify we get no errors with only a single event
        self.test_alert_compression(n_sensors=1, n_quakes=1, expect_one_packet=True)

    # def test_alert_compression_too_many_events(self):
        # with logging enabled, run this to verify that we'll receive an error msg
        # if we had too many new events (highest event ID seq #) to fit in one packet.
        # logging.basicConfig(level=logging.DEBUG)
        # self.test_alert_compression(n_sensors=100)

    # Helper functions used across multiple tests

    def _generate_events(self, n_unique_events, n_sources, n_duplicates=1,
                         event_type=None, time_incr=None, sensor_name="sensor%d"):
        """
        Generate the given number of events for each of the given number of sources.  Also generates a number of
        duplicate events for the requested number.
        :param event_type: if specified, sets the event's type/topic
        :param time_incr: if specified, generates each successive packet (for different unique events / duplicates, but
        not sources) with a timestamp that's time_incr seconds later
        :param sensor_name: a string that will be formatted with the sequential source number (e.g. default='sensor%d')
        and used as the events' source(s)
        """

        # TODO: if we change the APIs and need a more portable method of generating duplicate events, could create
        # multiple publisher instances and have each of them generate events, copying these events for each duplicate.

        events = []
        timestamp = time.time() if time_incr is not None else None
        for k in range(n_duplicates):
            for i in range(n_unique_events):
                for j in range(n_sources):
                    ev = self.pub.make_event(data=i, timestamp=timestamp, event_type=event_type, source=sensor_name % j)
                    events.append(ev)

                if timestamp is not None:
                    timestamp += time_incr

        return events

if __name__ == '__main__':
    unittest.main()
