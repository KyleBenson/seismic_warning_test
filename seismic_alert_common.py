SEISMIC_ALERT_TOPIC = 'seismic_alert'
SEISMIC_PICK_TOPIC = 'seismic'

# add topic to end of path
SUBSCRIPTION_API_PATH = '/subscriptions/'

from scale_client.util.uri import parse_uri

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
        raise ValueError("unable to determine unique ID for event source %s" % ev.source)
    return hostname
