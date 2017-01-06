__author__ = 'kebenson'

NEW_SCRIPT_DESCRIPTION = '''Simple aggregating server that receives 'picks' from clients
indicating a possible seismic event using a simple JSON format over UDP.
It aggregates together all of the readings over a short period of time and
then forwards the combined data to interested client devices.'''

# @author: Kyle Benson
# (c) Kyle Benson 2016

import logging as log
log.basicConfig(format='%(levelname)s:%(message)s', level=log.DEBUG)

import sys
import argparse
import time
import asyncore
import socket
import json
from threading import Timer


# Buffer size for receiving packets
BUFF_SIZE = 4096


def parse_args(args):
    ##################################################################################
    # ################      ARGUMENTS       ###########################################
    # ArgumentParser.add_argument(name or flags...[, action][, nargs][, const][, default][, type][, choices][, required][, help][, metavar][, dest])
    # action is one of: store[_const,_true,_false], append[_const], count
    # nargs is one of: N, ?(defaults to const when no args), *, +, argparse.REMAINDER
    # help supports %(var)s: help='default value is %(default)s'
    ##################################################################################

    parser = argparse.ArgumentParser(description=NEW_SCRIPT_DESCRIPTION,
                                     #formatter_class=argparse.RawTextHelpFormatter,
                                     #epilog='Text to display at the end of the help print',
                                     )

    parser.add_argument('--delay', '-d', type=float, default=5,
                        help='''time period (in secs) during which sensor readings
                         are aggregated before sending the data to interested parties''')
    parser.add_argument('--quit_time', '-q', type=float, default=30,
                        help='''delay (in secs) before quitting''')

    parser.add_argument('--recv_port', type=int, default=9999,
                        help='''UDP port number from which data should be received''')
    parser.add_argument('--send_port', type=int, default=9998,
                        help='''UDP port number to which data should be sent''')
    parser.add_argument('--address', '-a', type=str, default="127.0.0.1",
                        help='''IP address to which the aggregated data should be sent''')

    return parser.parse_args(args)


class SeismicServer(asyncore.dispatcher):

    def __init__(self, config):
        asyncore.dispatcher.__init__(self)

        # store configuration options and validate them
        self.config = config

        # Stores received events indexed by their 'id'
        self.events_rcvd = dict()

        # queue seismic event aggregation and forwarding
        # need to store references to cancel them when finish() is called
        self.next_timer = Timer(self.config.delay, self.send_events).start()

        # setup UDP network socket to listen for events on
        self.create_socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.bind(('', self.config.recv_port))

        # we assume that the quit_time is far enough in the future that
        # no interesting sensor data will arrive around then, hence not
        # worrying about flushing the buffer
        Timer(self.config.quit_time, self.finish).start()

    def send_events(self):
        if len(self.events_rcvd) > 0:
            agg_events = dict()
            # aggregated events are expected as an array
            agg_events['events'] = [v for v in self.events_rcvd.values()]
            agg_events['id'] = 'aggregator'
            # TODO: test and determine whether or not we need to lock the data structures
            self.sendto(json.dumps(agg_events), (self.config.address, self.config.send_port))
            log.info("Aggregated events sent")

        # don't forget to schedule the next time we send aggregated events
        self.next_timer = Timer(self.config.delay, self.send_events)
        self.next_timer.start()

    def handle_read(self):
        """
        Receive an event and record the time we received it.
        If it's already been received, we simply record the fact that
        we've received a duplicate.
        """

        data = self.recv(BUFF_SIZE)
        # ENHANCE: handle packets too large to fit in this buffer
        try:
            event = json.loads(data)
            log.info("received event %s" % event)

            # Aggregate events together in an array
            if event['id'] not in self.events_rcvd:
                event['time_aggd'] = time.time()
                self.events_rcvd[event['id']] = event

        except ValueError:
            log.error("Error parsing JSON from %s" % data)
        except IndexError as e:
            log.error("Malformed event dict: %s" % e)

    def run(self):
        try:
            asyncore.loop()
        except:
            # seems as though this just crashes sometimes when told to quit
            log.error("Error in SeismicServer.run() can't recover...")

    def finish(self):
        # need to cancel the next timer or the loop could keep going
        self.next_timer.cancel()
        self.close()

if __name__ == '__main__':
    args = parse_args(sys.argv[1:])
    client = SeismicServer(args)
    client.run()
