SEISMIC_ALERT_TOPIC = 'seismic_alert'
SEISMIC_PICK_TOPIC = 'seismic'
DATA_PATH_UPDATE_TOPIC = 'data_path_update'
PUBLISHER_ROUTE_TOPIC = 'publisher_route'

# add topic to end of path
SUBSCRIPTION_API_PATH = '/subscriptions/'

from scale_client.util.uri import parse_uri
from scale_client.networks.util import msg_fits_one_coap_packet, COAP_MAX_PAYLOAD_SIZE

import logging
log = logging.getLogger(__name__)

def get_hostname_from_path(path):
    """
    :param path:
    :return: an IPAddress of the host if present, else None
    """
    hostname = parse_uri(path).gethost()
    return str(hostname) if hostname is not None else None

# TODO: can add this back in if we figure out a way to get our IP address...
# def get_app_id(app):
#     """Application ID for uniquely identifying which host we're dealing with is just the IP address."""
#     _id = get_hostname_from_path(app.path)
#     if _id is None:

def get_event_source_id(ev):
    hostname = get_hostname_from_path(ev.source)
    if hostname is None:
        log.debug("Unable to extract hostname as unique ID for event source! Using original source instead: %s" % ev.source)
        return ev.source
    return hostname

def get_event_id(ev):
    """
    Returns an ID uniquely identifying the specified event.  An event is considered unique when it represents a unique
    detection by a physical sensor i.e. it comes from a different source node or has been marked as being a different
    real-world event by the source node by having a different sequence number.
    :type ev: scale_client.core.sensed_event.SensedEvent
    """
    eid = "%s/%s" % (get_event_source_id(ev), ev.data)
    # NOTE: these event IDs will be a minimum character-length of 9
    return eid

def get_source_from_event_id(ev_id):
    return ev_id.split('/')[0]

def get_seq_from_event_id(ev_id):
    """
    Extracts the sequence number from the given event ID used to represent an aggregated pick event.
    :param ev_id:
    :return:
    :rtype int:
    """
    return int(ev_id.split('/')[1])

# This is here mainly for ease of testing without importing RideDEventSink
def compress_alert_one_coap_packet(event):
    """
    Modifies and encodes the provided seismic alert SensedEvent so that its data contains the largest list of the most
    recent events that will all fit in a single CoAP packet.  The returned encoding of the SensedEvent will contain
    only the necessary fields for deserialization and use by the subscriber; the alert data will contain only event IDs.
    When analyzing results, we'll have to figure out the timestamps from the various output files...
    :param event:
    :type event: scale_client.core.sensed_event.SensedEvent
    :return: encoded event
    """

    assert event.event_type == SEISMIC_ALERT_TOPIC, "this method is designed solely for compressing %s events!" % SEISMIC_ALERT_TOPIC

    # IDEA: sort the list of seismic event IDs in event.data sequence number and continually remove the oldest one until
    # the JSON serialization of the event (with unnecessary fields excluded) fits into a single CoAP packet.

    # First, change and sort the event's data but plan on restoring it when we're done.  Note that if we were to sort
    # by time_sent we would have to do some more swapping stuff with the data so that when we modify the event.data and
    # serialize event we can restore the old event.data to keep checking timestamps.  Hence just sorting by seq #...
    old_event_data = event.data
    event.data = list(sorted(event.data.keys(), key=lambda e: get_seq_from_event_id(e), reverse=True))

    excluded_fields = ('schema', 'condition', 'misc', 'prio_value', 'prio_class', 'timestamp')
    comp_ev = event.to_json(exclude_fields=excluded_fields, no_whitespace=True)

    # To determine if we've excluded any events that are brand new, which we consider a problem that'd skew results:
    highest_seq = max(get_seq_from_event_id(eid) for eid in event.data)
    n_highest_seq_eids = len([eid for eid in event.data if get_seq_from_event_id(eid) == highest_seq])

    # XXX: since we know the maximum size each event ID will be, we can quickly chop off a bunch and save time on this
    # slow hacky serialize-and-check loop
    _longest_eid = max(len(eid) for eid in event.data) + 3    # JSON encoding will include quotes and a comma
    _bytes_over = len(comp_ev) - COAP_MAX_PAYLOAD_SIZE
    if _bytes_over >= _longest_eid:
        min_eids_over = _bytes_over / _longest_eid
        # log.debug("immediately trimming %d eids" % min_eids_over)
        event.data = event.data[:-min_eids_over]
        comp_ev = event.to_json(exclude_fields=excluded_fields, no_whitespace=True)

        assert len(comp_ev) + _longest_eid > COAP_MAX_PAYLOAD_SIZE,\
            "trimmed too many events! down to %d/%d but longest_eid is %d!\nEncoded Event: " \
            "%s" % (len(comp_ev), COAP_MAX_PAYLOAD_SIZE, _longest_eid, comp_ev)

    while not msg_fits_one_coap_packet(comp_ev):
        event.data.pop()
        comp_ev = event.to_json(exclude_fields=excluded_fields, no_whitespace=True)

    # Verify we didn't cut out any new events!
    if n_highest_seq_eids > len(event.data):
        log.error("cut out %d event IDs with highest sequence #!  This may mean seismic picks never being delivered"
                  " in alerts..." % (n_highest_seq_eids - len(event.data)))

    event.data = old_event_data
    return comp_ev