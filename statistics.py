#!/usr/bin/python

STATISTICS_DESCRIPTION = '''Gathers statistics from the output files in order to determine how long it
    took the events to reach the interested clients. Also handles printing them in
     an easily-read format as well as creating plots.'''

import numpy as np
import matplotlib.pyplot as plt
import argparse
import sys
import os
import json
import logging as log


class SeismicStatistics(object):
    """Gathers statistics from the output files in order to determine how long it
    took the events to reach the interested clients. Also handles printing them in
    an easily-read format as well as creating plots.

    General strategy:
    -- Each directory represents a single experiment
    -- Each file in that directory represents the readings received by a single subscriber
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

        log_level = log.getLevelName(debug.upper())
        log.basicConfig(format='%(levelname)s:%(message)s', level=log_level)

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

        parser.add_argument('--dirs', '-d', type=str, nargs="+", default=['output'],
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

    def parse_all(self, dirs_to_parse=None, stats=None):
        """
        Parse all of the requested directories, save the stats as self.stats, and return them.
        :param dirs_to_parse: list of directories to parse (self.dirs if None)
        :param stats: dict in which to store the parsed stats (self.stats if None)
        :return stats: parsed stats dict
        """
        if dirs_to_parse is None:
            dirs_to_parse = self.dirs
        if stats is None:
            stats = self.stats
        for dirname in dirs_to_parse:
            stats[dirname] = self.parse_dir(dirname)
        return stats

    def parse_dir(self, dirname):
        results = dict()
        for filename in os.listdir(dirname):
            parsed = self.parse_file(os.path.join(dirname, filename))
            results[filename] = parsed
        return results

    def parse_file(self, fname):
        with open(fname) as f:
            return json.loads(f.read())

    @classmethod
    def get_publishers(cls, group):
        return cls.split_publishers_subscribers(group)[0]
    @classmethod
    def get_subscribers(cls, group):
        return cls.split_publishers_subscribers(group)[1]

    @classmethod
    def split_publishers_subscribers(cls, group):
        pubres = dict()
        subres = dict()
        for group_name, results in group.items():
            # XXX: we assume the group's name/key is a filename starting with either publisher or subscriber
            if group_name.startswith('publisher'):
                pubres[group_name] = results
            elif group_name.startswith('subscriber'):
                subres[group_name] = results
            else:
                log.error("expected group/file name to start with either publisher or subscriber to identify them! instead got: %s" % group_name)

        return pubres, subres

    @classmethod
    def get_latencies(cls, group):
        """Returns the latencies (delta from time originally sent by publisher until
        when the subscriber received the event) for this group of results
        (directory of client outputs).
        :param group: dictionary of {filename: parsed_results} pairs representing the
        results of a single experiment in terms of seismic client outputs
        :return latencies: where len(latencies) = num_sensors * num_subscribers,
         assuming all subscribers received all publications
        :rtype: np.array[float]
        """

        latencies = []
        subs_results = cls.get_subscribers(group).values()
        for sub_results in subs_results:
            for event in sub_results.values():
                time_rcvd = event['time_rcvd']
                time_sent = event['time_sent']
                latencies.append(time_rcvd - time_sent)

        return np.array(latencies)

    @classmethod
    def get_reachability(cls, group, nsubscribers=None):
        """
        Gets the 'reachability' of the group, which is the normalized # sensors
        that received any aggregated results messages, which are assumed to actually
        be some kind of alert.  Thus, this doesn't consider receiving the 'original'
        event, but just the aggregated one from the server.
        :param group: dictionary of {filename: parsed_results} pairs representing the
        results of a single experiment in terms of seismic client outputs
        :param int nsubscribers: # subscribers being considered (if None, calculates
        it as len(all subscribers in group))
        :return: reachability
        :rtype float:
        """

        subscribers = cls.get_subscribers(group)

        # First, we need to determine nsubscribers if not specified
        if nsubscribers is None:
            nsubscribers = len(subscribers)
            assert nsubscribers > 0, "0 subscribers for group: %s" % group

        # Now we just count the # subscribers that actually received SOME events and then normalize
        reachability = sum(len(results) > 0 for results in subscribers.values())
        reachability /= float(nsubscribers)
        assert reachability <= 1.0, "reachability should be in range [0,1]!!"
        return reachability

    @classmethod
    def plot_cdf(cls, stats, num_bins=10):
        """Plots the CDF of the number of seismic events received over time
         at interested clients. Averaged over all subscribers.
        :param dict stats: dict of <group_name: group_stats> pairs where each group is an experimental treatment
        """

        # TODO: use these markers
        # markers = 'x.*+do^s1_|'
        # plot(..., marker=markers[i%len(markers)])

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
                plt.plot(bin_edges[1:], cdf, label=cls.get_label_for_group(group_name))
            except ZeroDivisionError:
                log.error("Group %s (%d pubs; %d subs) had ZeroDivisionError and was skipped. len(group)=%d" % (group_name, npublishers, nsubscribers, len(group)))

        plt.xlabel("time(secs)")
        # TODO: put x scale as log? maybe y too?
        plt.ylabel("avg % readings rcvd")
        plt.title('Sensor readings received over time')
        plt.legend(loc=0)  # loc=0 --> auto
        plt.show()

    @classmethod
    def get_label_for_group(cls, group_name):
        """
        Returns a human-readable label for the given group_name, which is assumed
        to be a path to the directory containing the client output files.  Hence
        we just return the last part of that path.
        :param group_name:
        :return:
        """
        return os.path.split(group_name)[-1]

    def plot_time(self):
        """Plots the events' latencies over time.  Useful for
        determining if event processing slows down over time or spikes
        at a certain point.  CURRENTLY NOT IMPLEMENTED"""

        raise NotImplementedError
        # TODO: fix the code below as it appears to have been copy/pasted from a different analyzer file

        # Rather than raw datetimes, we want to use the total seconds since the
        # simulation's start
        timestamps = [e.timestamp for e in self.events]
        tmin = min(timestamps)
        timestamps = [(t-tmin).total_seconds() for t in timestamps]

        plt.plot(timestamps, self.get_latencies())
        plt.xlabel("time(s)")
        plt.ylabel("latency(ms)")
        plt.title('Event latency over time')
        plt.legend(loc=0)  # loc=0 --> auto
        plt.show()

    def print_statistics(self):
        """Prints summary statistics for all groups, in particular
        the mean latency and standard deviation."""

        for group_name, group in self.stats.items():
            latencies = self.get_latencies(group)
            print "Group %s's latency Mean: %fs; stdev: %fs" % (group_name, np.mean(latencies), np.std(latencies))

            reach = self.get_reachability(group)
            print "Group %s's reachability: %f" % (group_name, reach)

    # could use this if we ever put the events into some kind of object
    # def __repr__(self):
    #     s = "SensedEvent (%s) with value %s" % (self.get_type(), str(self.get_raw_data()))
    #     s += pprint.pformat(self.data, width=1)
    #     return s


if __name__ == '__main__':
    stats = SeismicStatistics.build_from_args(sys.argv[1:])
    stats.parse_all()
    stats.print_statistics()
    stats.plot_cdf(stats.stats)