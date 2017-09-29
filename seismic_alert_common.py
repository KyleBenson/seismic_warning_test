SEISMIC_ALERT_TOPIC = 'seismic_alert'
SEISMIC_PICK_TOPIC = 'seismic'
DATA_PATH_UPDATE_TOPIC = 'data_path_update'
PUBLISHER_ROUTE_TOPIC = 'publisher_route'

# add topic to end of path
SUBSCRIPTION_API_PATH = '/subscriptions/'

from scale_client.util.uri import parse_uri

import struct
import ipaddress
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
    return eid

def get_source_from_event_id(ev_id):
    return ev_id.split('/')[0]

def get_seq_from_event_id(ev_id):
    return int(ev_id.split('/')[1])

# NOTE: this might disappear if we dynamically support different event IDs...
PACKED_EVENT_ID_LEN = 5

def pack_event_id(ev_id):
    # ENHANCE: if event source is not IPv4 address or seq # is > 255, we'll have to check that and do something different
    packed_ipv4 = ipaddress.IPv4Address(unicode(get_source_from_event_id(ev_id))).packed
    packed_seq = struct.pack('B', get_seq_from_event_id(ev_id))
    return packed_ipv4 + packed_seq

def unpack_event_id(ev_id):
    ipv4 = str(ipaddress.IPv4Address(ev_id[:4]))
    seq = int(struct.unpack('B', ev_id[-1])[0])
    return "%s/%d" % (ipv4, seq)

def pack_seismic_alert_data(event_data):
    """Encodes the expected event data (a list of event IDs) to binary format."""
    return ''.join(pack_event_id(i) for i in event_data)

def unpack_seismic_alert_data(event_data):
    """Unpacks the binary list of event IDs stored in event_data and returns them in the expected event ID format."""
    assert len(event_data) % PACKED_EVENT_ID_LEN == 0, "list of compressed event IDs should be a multiple of %d!" % PACKED_EVENT_ID_LEN

    # IDEA: continually read enough bytes from event_data to unpack a single event ID, gathering them up in a list

    ev_ids = []
    start = 0
    while start < len(event_data):
        the_bytes = event_data[start : start + PACKED_EVENT_ID_LEN]
        evid = unpack_event_id(the_bytes)
        ev_ids.append(evid)
        start += PACKED_EVENT_ID_LEN

    # make sure we got everything!
    assert len(ev_ids) == len(event_data) / PACKED_EVENT_ID_LEN

    return ev_ids
