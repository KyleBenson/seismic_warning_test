SEISMIC_ALERT_TOPIC = 'seismic_alert'
SEISMIC_PICK_TOPIC = 'seismic'
DATA_PATH_UPDATE_TOPIC = 'data_path_update'
PUBLISHER_ROUTE_TOPIC = 'publisher_route'

# add topic to end of path
SUBSCRIPTION_API_PATH = '/subscriptions/'

from scale_client.util.uri import parse_uri

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
