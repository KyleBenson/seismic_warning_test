#!/usr/bin/python

STATISTICS_DESCRIPTION = '''Gathers statistics from the output files in order to determine how long it
    took the events to reach the interested clients. Also handles printing them in
     an easily-read format as well as creating plots.'''

import numpy as np
import pandas as pd
import parse
import argparse
import sys
import os
import json
import logging
log = logging.getLogger(__name__)

from scale_client.core.sensed_event import SensedEvent
from seismic_alert_common import *
from config import get_ip_mac_for_host

DEFAULT_TIMEZONE='America/Los_Angeles'


def get_host_ip(hid):
    # XXX: we only have the host ID but not IP address, so just build it with our helper functions, noting that
    # the edge/cloud server names are essentially all s0/x0

    if hid == 'srv' or hid == 'edge':
        hid = 's0'
    elif hid == 'cloud':
        hid = 'x0'

    hip = get_ip_mac_for_host(hid)[0]
    # XXX: host_ip includes subnet mask #bits
    hip = hip.split('/')[0]

    return hip


class ParsedSeismicOutput(pd.DataFrame):
    """
    Parses the output from one of the seismic client apps running in the scale_client and stores it in a pandas.DataFrame
    for later manipulation and aggregation with the other client outputs.

    NOTE: any columns with labels matching 'time*' will be converted to pandas.Timestamp with the expectation that
    they're in Unix epoch format!
    """

    def __init__(self, data, host_id=None, **kwargs):
        """
        Parses the raw JSON data into a dict of events recorded by the client and passes the resulting data into
        a pandas.DataFrame with additional columns specified by kwargs, which should include e.g. host_id, run#, etc.
        :param data: raw json string of event data the client recorded
        :type data: str
        :param kwargs: additional static values for columns to distinguish this group of events from others e.g. host_id
        """
        # NOTE: can't save any attributes until after we run super constructor!

        data = json.loads(data)
        # sub-classes may extract the column data in different ways depending on the output format
        columns = self.extract_columns(data)
        columns.update(kwargs)

        if host_id is not None:
            # also make sure to keep the host_id column if requested!
            columns['host_id'] = host_id
            # XXX: since clients don't always know their IP address, we can just translate their ID to it
            columns['host_ip'] = get_host_ip(host_id)

        # XXX: convert known columns to more specific pandas data types
        for k, val in columns.items():
            # XXX: any columns starting with 'time' should be converted to pandas.Timestamp!
            if k.startswith('time'):
                columns[k] = pd.to_datetime(val, unit='s')
                columns[k].tz = DEFAULT_TIMEZONE
            # TODO: figure out how to take a single value and share it through all rows as a category when it's just a string.
            # BUT: we can't have the values of a category be null so how to do this with picks and e.g. 'sink'???
            # XXX: basically everything else that isn't numeric is a category
            # elif k.endswith('_ip') or k.endswith('_id'):
            # elif isinstance(k, basestring):
            #     columns[k] = pd.Series(val, dtype='category')

        super(ParsedSeismicOutput, self).__init__(columns)

        # self.host_type = kwargs.get('host_type', self.__class__.__name__)

    @classmethod
    def extract_columns(cls, data):
        raise NotImplementedError

    def __str__(self):
        return "%s parsed results:\n%s" % (self.__class__.__name__, super(ParsedSeismicOutput, self).__str__())


class SensedEventParsedSeismicOutput(ParsedSeismicOutput):
    """
    The events contained in the output are formatted as a list of SCALE SensedEvents
    """

    @classmethod
    def extract_columns(cls, data):
        """
        Extracts the important columns from the given list of SensedEvents
        :param data:
        :return:
        """
        events = [SensedEvent.from_map(e) for e in data]
        cols = {'topic': [ev.topic for ev in events],
                'time_sent': [ev.timestamp for ev in events],
                # TODO: might not even want this? what to do with it? the 'scale-local:/' part makes it less useful...
                'source': [ev.source for ev in events],
                # all the events these types receive are seq#s; could move this down to derived class if needed...
                'seq': [ev.data for ev in events],
                }
        return cols


class PublisherOutput(SensedEventParsedSeismicOutput):
    """Output from a single SeismicAlertPublisher process, which is just a list of SensedEvents"""
    pass
    # @classmethod
    # def extract_columns(cls, data):
    #     """Change the column name for 'data'"""
    #     cols = super(PublisherOutput, cls).extract_columns(data)
    #     cols['seq'] = cols.pop('data')
    #     return cols


class ServerOutput(SensedEventParsedSeismicOutput):
    """Output from a SeismicAlertServer, which is also just a list of SensedEvents.  The seismic events were received,
     stamped with the time they were aggregated, and later sent out as alerts.  The generic IoT data events are just
     stored as is."""

    @classmethod
    def extract_columns(cls, data):
        """We need to extract the time each pick was received at the server for processing"""
        cols = super(ServerOutput, cls).extract_columns(data)
        cols['time_rcvd'] = [cls.get_aggregation_time(ev) for ev in (SensedEvent.from_map(e) for e in data)]
        cols['src_ip'] = [get_hostname_from_path(src) for src in cols.pop('source')]
        return cols

    @staticmethod
    def get_aggregation_time(event):
        """
        :type event: SensedEvent
        """
        # XXX: we didn't include this timestamp in generic iot data originally, so return None if so...
        return event.metadata.get("time_aggd")


class SubscriberOutput(ParsedSeismicOutput):
    """
    Output from a single SeismicAlertSubscriber instance, which is quite different from the other apps.
    It's a dictionary mapping event_ids to event data: time_rcvd, copies_rcvd, and agg_src (edge or cloud server).
    We'll store these events in a similar manner but split up the event_id into event sequence # and source host so
    that we can get the events by quake ID and/or source host
    """

    @classmethod
    def extract_columns(cls, data):
        return {'seq': [get_seq_from_event_id(event_id) for event_id, metadata in data.items()],
                'pub_ip': [get_source_from_event_id(event_id) for event_id, metadata in data.items()],
                'time_alert_rcvd': [metadata['time_rcvd'] for event_id, metadata in data.items()],
                'copies_rcvd': [metadata['copies_rcvd'] for event_id, metadata in data.items()],
                'alert_src': ['edge' if "Edge" in metadata['agg_src'] else 'cloud' for event_id, metadata in data.items()],
                # TODO: add artificial topic column for merging?
                }


class SeismicStatistics(object):
    """Gathers statistics from the output files in order to determine how long it
    took the events to reach the interested clients. Also handles printing them in
    an easily-read format as well as creating plots.

    General strategy:
    -- Each directory represents a single experiment
    -- Each file in that directory represents the events output from a single client (publications,
       alerts received by subscribers, etc.)
    -- Each reading tells us when it was first sent and when it was received
    -- So each time step has a number of readings received by this client
    -- Hence we should average over this # readings across all clients
    -- We should then plot the CDF of these averages to see how well the experiment performed
    """

    def __init__(self, dirs, debug='info'):
        """
        Constructor.
        :param dirs: list of directories to parse all the contained results files in
        :type List[str] dirs:
        """
        super(self.__class__, self).__init__()
        self.dirs = dirs

        # store all the parsed stats indexed by directory name, then by filename
        self.stats = dict()

        log_level = logging.getLevelName(debug.upper())
        log.setLevel(log_level)

    @classmethod
    def get_arg_parser(cls):
        ##################################################################################
        #################      ARGUMENTS       ###########################################
        # ArgumentParser.add_argument(name or flags...[, action][, nargs][, const][, default][, type][, choices][, required][, help][, metavar][, dest])
        # action is one of: store[_const,_true,_false], append[_const], count
        # nargs is one of: N, ?(defaults to const when no args), *, +, argparse.REMAINDER
        # help supports %(var)s: help='default value is %(default)s'
        ##################################################################################

        parser = argparse.ArgumentParser(description=STATISTICS_DESCRIPTION,
                                         # formatter_class=argparse.RawTextHelpFormatter,
                                         # epilog='Text to display at the end of the help print',
                                         )

        parser.add_argument('--dirs', '-d', type=str, nargs="+", default=['results'],
                            help='''directories containing files from which to read outputs
                            (default=%(default)s)''')
        parser.add_argument('--debug', '--verbose', '-v', type=str, default='info', nargs='?', const='debug',
                            help='''set verbosity level for logging facility (default=%(default)s, %(const)s when specified with no arg)''')

        return parser

    @classmethod
    def build_from_args(cls, args):
        parser = cls.get_arg_parser()
        args = parser.parse_args(args)
        args = vars(args)
        return cls(**args)

    def parse_all(self, dirs_to_parse=None, stats=None, **metadata):
        """
        Parse all of the requested directories, save the stats as self.stats, and return them.
        :param dirs_to_parse: list of directories to parse (self.dirs if None)
        :param stats: dict in which to store the parsed stats (self.stats if None)
        :param metadata: static column data to add to these parsed results
        :return stats: parsed stats dict
        """
        if dirs_to_parse is None:
            dirs_to_parse = self.dirs
        if stats is None:
            stats = self.stats
        for dirname in dirs_to_parse:
            log.debug("parsing directory %s" % dirname)
            if dirname in stats:
                log.warning("directory '%s' already in stats!" % dirname)
            stats[dirname] = self.parse_dir(dirname, **metadata)
        return stats

    def parse_dir(self, dirname, **metadata):
        # TODO: separate into pubs, subs, srv?
        results = dict()
        for filename in os.listdir(dirname):
            parsed = self.parse_file(os.path.join(dirname, filename), **metadata)
            if parsed is not None:
                if parsed.empty:
                    log.warning("file %s returned empty results!" % filename)
                else:
                    log.debug('parsed file %s; returned %s results with head: %s' % (filename, parsed.__class__.__name__, parsed.head()))
                results[filename] = parsed
        return results

    def parse_file(self, fname, **metadata):
        with open(fname) as f:
            data = f.read()

            # Build up the kwargs that we'll use to label data columns with run#, host_id, etc.
            path_parts, fname = os.path.split(fname)
            # the files themselves are stored in a directory for that experiment run, with each of these run dirs
            # appearing under a directory named for the experimental treatment
            path_parts, run_dir = os.path.split(path_parts)
            path_parts, treatment = os.path.split(path_parts)
            treatment = treatment.replace('outputs_', '')  # the dir usually starts with this, so just cut it off
            try:
                run = parse.parse('run{:d}', run_dir)[0]
            except IndexError:
                run = 0
            # we parse using these indices in case the fname is e.g. srv
            host_id = fname.split('_')[-1]
            host_type = fname.split('_')[0]
            host_type = 'edge' if host_type == 'srv' else host_type  # translate name

            cols = dict(host_id=host_id,                      # e.g. h0-b12, srv, cloud
                        host_type=host_type,                  # e.g. publisher, congestor, subscriber, srv, cloud
                        run=run,
                        )
            if treatment:
                cols['treatment'] = treatment
            # TODO: make the values in cols all categories?  certainly shouldn't be null.... unless we merge with non-mininet version?
            cols.update(metadata)

            if host_type == 'subscriber':
                return SubscriberOutput(data, **cols)
            elif host_type == 'publisher':
                return PublisherOutput(data, **cols)
            elif host_type == 'congestor':
                return PublisherOutput(data, **cols)
            elif host_type == 'edge' or host_type == 'cloud':
                return ServerOutput(data, **cols)
            else:
                log.error("skipping unrecognized output file type with name: %s" % fname)
                return None


    #########################################################################################################
    ####          FILTERING,  AGGREGATION,   and METRICS
    ### Helper functions for getting parsed outputs from certain types of clients
    ## Gathering certain types of results, which becomes hierarchical to build up to our final results
    #########################################################################################################

    # Helper function for merging together >2 data frames into one
    def merge_all(self, *dfs):
        """
        Merges every data frame in dfs until only one is left.  This uses the 'reduce' function, 'outer' join,
        and doesn't sort the rows.
        :param dfs:
        :return:
        """
        return reduce(lambda left, right: pd.merge(left, right, how='outer', sort=False), dfs)

    ## Level 0: filter by treatment group (directory name) and arbitrary column parameters

    # This will be used for the others
    def filter_outputs_by_params(self, group=None, include_empty=False, **param_matches):
        """Filter the ParsedOutputs by the values of the specified parameters including the group
         (experimental treatment) name.  The param_matches keys are columns and
        its values are the values all elements of that column should have (static data).
        If you want to filter ranges of the data, just use the pandas DataFrame operations.

        NOTE: this also filters out any empty data frames!
        :rtype: pd.DataFrame
        """
        if group is None:
            groups = self.stats.items()
        else:
            groups = ((group, self.stats[group]),)
        return [parsed for group, file_results in groups for fname, parsed in file_results.items() if
                (not parsed.empty or include_empty) and all((parsed[k] == v).all() for k, v in param_matches.items())]

    # TODO: make filter handle operations other than ==  ?
    filter = filter_outputs_by_params


    # Levels 0-1: filtering by treatment group then by output type (from what host type)

    def subscribers(self, **kwargs):
        """:rtype: pd.DataFrame"""
        return self.filter_outputs_by_params(host_type='subscriber', **kwargs)
    def publishers(self, **kwargs):
        """:rtype: pd.DataFrame"""
        return self.filter_outputs_by_params(host_type='publisher', **kwargs)
    def iot_congestors(self, **kwargs):
        """:rtype: pd.DataFrame"""
        return self.filter_outputs_by_params(host_type='congestor', **kwargs)
    def edge_servers(self, **kwargs):
        """:rtype: pd.DataFrame"""
        return self.filter_outputs_by_params(host_type='edge', **kwargs)
    def cloud_servers(self, **kwargs):
        """:rtype: pd.DataFrame"""
        return self.filter_outputs_by_params(host_type='cloud', **kwargs)

    # Level 2: combining the different outputs to view events flowing through the system

    def picks(self, **kwargs):
        """
        The seismic pick messages sent from publishers-->server
        :returns: a single pandas.DataFrame containing all of the picks, filtered by the optional parameter equalities
        :rtype: pd.DataFrame
        """
        # IDEA: merge all the publication data so we have unique source, seq# keys;
        # merge cloud/edge server, then join these two tables so we have time_sent, time_rcvd, and sink=cloud/srv
        pubs = self.publishers(**kwargs)
        # also filter out the generic_iot_data from these
        clouds = [df[df.topic == SEISMIC_PICK_TOPIC] for df in self.cloud_servers(**kwargs)]
        edges = [df[df.topic == SEISMIC_PICK_TOPIC] for df in self.edge_servers(**kwargs)]

        # merge the lists down to a single DataFrame
        pubs = self.merge_all(*pubs)
        servers = self.merge_all(*(clouds + edges))

        # before joining the two tables, handle removing/renaming some columns that would cause conflicts:
        # - all useless: pubs.source, servers.host_ip/id, pubs.host_type
        # - servers.src_ip is the pub's host_ip
        # - host_type would conflict, but we'll use the servers' column as 'sink'
        del pubs['source']
        del pubs['host_type']
        del servers['host_ip']
        del servers['host_id']
        pubs.rename(columns=dict(host_ip='src_ip', host_id='src_id'), inplace=True)
        servers.rename(columns=dict(host_type='sink'), inplace=True)

        # Do a left join so we know which picks were sent even if they weren't received
        picks = pubs.merge(servers,  how='left')
        return picks

    def alerts(self, **kwargs):
        """
        The seismic alerts sent from the server, which aggregated all the recent picks, to the subscribers
        :rtype: pd.DataFrame
        """
        # same as with picks() but with subscribers now, alert_src=cloud/srv, and time_sent being from the metadata alert_time
        subs = self.subscribers(**kwargs)
        # also filter out the generic_iot_data from these
        clouds = [df[df.topic == SEISMIC_PICK_TOPIC] for df in self.cloud_servers(**kwargs)]
        edges = [df[df.topic == SEISMIC_PICK_TOPIC] for df in self.edge_servers(**kwargs)]

        # merge the lists down to a single DataFrame
        subs = self.merge_all(*subs)
        servers = self.merge_all(*(clouds + edges))

        # before joining the two tables, handle removing/renaming some columns that would cause conflicts:
        # - all useless: servers.host_ip/id, subs.host_type
        # - servers.src_ip is the pub's host_ip
        # - host_type would conflict, but we'll use the servers' column as 'sink'
        del subs['host_type']
        del servers['host_ip']
        del servers['host_id']
        del servers['time_sent']  # just for picks
        del servers['topic']
        subs.rename(columns=dict(host_ip='sub_ip', host_id='sub_id'), inplace=True)
        servers.rename(columns=dict(host_type='alert_src', time_rcvd='time_alert_sent', src_ip='pub_ip'), inplace=True)

        # Do right join so we know what alerts were sent even if not received
        alerts = subs.merge(servers,  how='right', sort=False)
        return alerts

    def iot_traffic(self, **kwargs):
        """Generic constant-rate IoT background traffic send from publishers-->server
        :rtype: pd.DataFrame"""
        # same as with picks just with different topics
        pubs = self.iot_congestors(**kwargs)
        clouds = [df[df.topic == IOT_GENERIC_TOPIC] for df in self.cloud_servers(**kwargs)]
        edges = [df[df.topic == IOT_GENERIC_TOPIC] for df in self.edge_servers(**kwargs)]

        # merge the lists down to a single DataFrame
        pubs = self.merge_all(*pubs)
        servers = self.merge_all(*(clouds + edges))

        # before joining the two tables, handle removing/renaming some columns that would cause conflicts:
        # - all useless: pubs.source, servers.host_ip/id, pubs.host_type
        # - servers.src_ip is the pub's host_ip
        # - host_type would conflict, but we'll use the servers' column as 'sink'
        del pubs['source']
        del pubs['host_type']
        del servers['host_ip']
        del servers['host_id']
        pubs.rename(columns=dict(host_ip='src_ip', host_id='src_id'), inplace=True)
        servers.rename(columns=dict(host_type='sink'), inplace=True)

        # Do a left join so we know which data was sent even if it wasn't received
        data = pubs.merge(servers,  how='left', sort=False)
        return data

    # Level 3: seismic events end-to-end view: combine picks/alerts

    def seismic_events(self, **kwargs):
        # IDEA: just use our previous two helpers, but with slightly modified columns
        picks = self.picks(**kwargs)
        alerts = self.alerts(**kwargs)

        picks.rename(columns=dict(src_id='pub_id', src_ip='pub_ip', sink='alert_src',
                                  time_sent='time_pick_sent', time_rcvd='time_pick_rcvd'), inplace=True)

        # need outer join to merge all the parts together
        events = picks.merge(alerts, how='outer', sort=False)
        return events

    # Level 4: calculating metrics, which can then be viewed over time, by quake ID, etc.

    def latencies(self, data, resolution='ms'):
        """
        Adds a column to the given DataFrame with the latency (timedelta from sending time to receiving time), which
        is in the optionally requested resolution.  The 'sending time' and 'receiving time' are calculated by trying
        to find the difference of the relevant attributes in the following order:
        1) seismic_events are 'time_pick_sent/time_alert_rcvd'
        2) picks/traffic are 'time_sent/time_rcvd'
        3) alerts are 'time_alert_sent/time_alert_rcvd'

        :param data: the data to compute latencies on
        :type data: pd.DataFrame
        :param resolution: str representing the timedelta units/resolution e.g. ms, s, 10ms
        NOTE: resolution='10ms' would mean 1sec-->100; 13ms->1
        :rtype: pd.DataFrame
        """

        # XXX: just try finding the 3 different latency types in increasing generality
        try:
            # seismic events
            data['latency'] = (data.time_alert_rcvd - data.time_pick_sent).astype('timedelta64[%s]' % resolution)
        except AttributeError:
            try:
                # alerts
                data['latency'] = (data.time_alert_rcvd - data.time_alert_sent).astype('timedelta64[%s]' % resolution)
            except AttributeError:
                try:
                    # picks / traffic
                    data['latency'] = (data.time_rcvd - data.time_sent).astype('timedelta64[%s]' % resolution)
                except AttributeError:
                    raise AttributeError("data columns not recognized: expected to find time[_alert|_pick]_<sent|rcvd>")

        # TODO: we can only do resampling when the INDEX is time, so we'd have to convert it to that first...
        # could help us look at only the events during a certain quake period?
        # print 'LATENCY RESAMPLED:', picks['latency'].astype('timedelta64[ms]').resample('10ms')
        return data

    # TODO: sample the latencies by quake period using pd.cut with the bin edges being the quake times (dp change times)

    def reachabilities(self, **kwargs):
        """
        Filters the requested outputs down to just the unique combination of (treatment, run#, seq) and adds a column
        with the 'reachability' of the group, which is the normalized # sensors that received any alerts.
        Thus, this doesn't consider receiving the 'original' event, but just the aggregated one from the server.
        :rtype: pd.DataFrame
        """

        # IDEA: we just need alerts, but we need to know how many subscribers there were in total: hence getting empties
        alerts = self.alerts(include_empty=True, **kwargs)
        # ignore seq here since we just care about len(subs); drop anything without subs info; group by just
        # run/treatment and count up the #subs
        # NOTE: we do reset_index to go back to a DataFrame rather than MultiIndex since we'll be joining on multiple columns
        nsubs = alerts.drop_duplicates(['sub_id', 'treatment', 'run']).dropna(subset=['sub_id']).groupby(['treatment', 'run'], as_index=False).size().reset_index()
        # print 'counts:\n', nsubs[(0,"2t_0.15f_5s_5p_steiner_disjoint_campustopo_0.00e_importance")]

        # now determine the # unique sub_ids/ips for a given treatment,run#,event_id(seq) combo: could throw out publisher info
        # QUESTION: should we also average across the # runs?
        reached = alerts.drop_duplicates(['sub_id', 'treatment', 'run', 'seq']).dropna(subset=['sub_id']).groupby(['treatment', 'run', 'seq'], as_index=False).size().reset_index()

        # Now we rename last column, merge them, convert one of them into floats, add the new 'reachability' column
        # as subs_reached/nsubs, cut out the old columns, and return the result
        nsubs.rename(columns={nsubs.columns[-1]: 'total_subs'}, inplace=True)
        reached.rename(columns={reached.columns[-1]: 'subs_reached'}, inplace=True)
        result = pd.merge(nsubs, reached, on=['treatment', 'run'], how='outer')
        result['reachability'] = result.subs_reached.astype('float64')/result.total_subs
        del result['subs_reached']
        del result['total_subs']

        # TODO: should probably support filtering by latency?  can't really count a subscriber as reached if it takes a minute...
        return result

    # TODO: what else? service availability?

    def runs(self, run_num):
        return self.filter_outputs_by_params(run=run_num)

    # TODO: probably just use pandas to do this?

    @classmethod
    def get_cdf(cls, stats, num_bins=10):
        """Gets the CDF of the number of seismic events received over time at interested clients.
        Averaged over all subscribers.
        :param dict stats: dict of <group_name: group_stats> pairs where each group is an experimental treatment
        """

        for (group_name, group) in stats.items():
            latencies = cls.get_latencies(group)
            nsubscribers = len(cls.get_subscribers(group))
            npublishers = len(cls.get_publishers(group))
            log.debug("Group %s has %d sensors" % (group_name, npublishers))
            try:
                # Adjust the weight to account for the fact that each latency is a delta
                # for a publisher->subscriber "cross-product" combination.
                weight_adjustment = [1.0/npublishers/nsubscribers] * len(latencies)
                counts, bin_edges = np.histogram(latencies, bins=num_bins, weights=weight_adjustment)
                cdf = np.cumsum(counts)
                # plt.plot(bin_edges[1:], cdf, label=cls.get_label_for_group(group_name))
            except ZeroDivisionError:
                log.error("Group %s (%d pubs; %d subs) had ZeroDivisionError and was skipped. len(group)=%d" % (group_name, npublishers, nsubscribers, len(group)))

    def print_statistics(self):
        """Prints summary statistics for all groups, in particular
        the mean latency and standard deviation."""

        for group_name, group in self.stats.items():
            latencies = self.get_latencies(group)
            print "Group %s's latency Mean: %fs; stdev: %fs" % (group_name, np.mean(latencies), np.std(latencies))

            reach = self.get_reachability(group)
            print "Group %s's reachability: %f" % (group_name, reach)

    def output_data(self):
        """Writes the data to a file that can be imported to e.g. plot.ly"""
        # TODO: what are we going to output?  certainly not the raw data right?  first collect all event data/timestamps?
        pass


if __name__ == '__main__':
    logging.basicConfig()  # needed when run standalone

    # lets you print the data frames out on a wider screen
    pd.set_option('display.max_columns', 15)  # seismic_events has 14 columns
    pd.set_option('display.width', 2500)

    stats = SeismicStatistics.build_from_args(sys.argv[1:])
    stats.parse_all()

    ### some simple test output

    # for outs in stats.subscribers():
    # for outs in stats.publishers():
    # for outs in stats.iot_congestors():
    # for outs in stats.edge_servers():
    # for outs in stats.cloud_servers():
    # NOTE: the ones below are all aggregated to a single DF!
    # for outs in [stats.picks()]:
    # for outs in [stats.alerts()]:
    # for outs in [stats.iot_traffic()]:
    # for outs in [stats.seismic_events()]:
    for outs in [stats.latencies(stats.seismic_events(), resolution='ms')]:
    # for outs in [stats.reachabilities()]:
        # WARNING: can't do fancy comparison slicing with an empty data frame!
        print outs.info()
        print outs

        # print 'edge alerts:\n', outs[outs.alert_src == 'edge']
        # print "edge-sunk iot traffic:\n", outs[(outs.sink == 'edge')]
        # if outs.empty:
        #     print 'EMPTY OUTPUT:', outs
        #     continue
        # filt = outs[outs['topic'] == 'seismic']
        # print outs.loc[1:4,['host_ip', 'host_type', 'host_id']]


    #### COOKBOOK recipes for various operations we may want to perform later

    ## handling duplicated picks e.g. from sending a pick CON and continually retransmitting because ACK never processed properly
    # no_dups = pubs.merge(servers.drop_duplicates(['src_ip', 'run', 'seq']), how='left')
    # print 'NO DUPES:\n', no_dups
    # print 'THE DUPES:\n', servers[servers.duplicated(['src_ip', 'run', 'seq'], False)]
    # print "non-null PICKS:\n", len(outs[pd.notnull(outs.sink)])

    ## Using DataFrame.query() (this came from our attempt to make everything merged into a single data frame
    # can't run empty query
    # if not param_matches:
    #     return self.stats
    # query_string = ' & '.join(('%s == "%s"' % (k, v) for k, v in param_matches.items()))
    # log.debug("running query: %s" % query_string)
    # return self.stats.query(query_string)

    #### WARNINGS to watch out for during data analysis:
    #
    # Alerts may appear to have a mis-matched # from cloud vs. edge since a subscriber might receive it from cloud first,
    #   the pub retransmits due to lack of ACK, it gets to the edge at some point and is then alerted out to subs, which
    #   won't record it since they've already received one from the cloud!