__author__ = 'kebenson'

NEW_SCRIPT_DESCRIPTION = '''Simple client that sends a 'pick' indicating a possible seismic event to a server using
a simple JSON format over UDP.'''

# @author: Kyle Benson
# (c) Kyle Benson 2016

import sys
import argparse
import time
import asyncore
import socket
import json
import os


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


    parser.add_argument('--file', '-f', type=str, default="output_events_rcvd",
                        help='''file to write statistics on when picks were sent/recvd to
                        (Note that it appends a '_' and the id to this string)''')
    parser.add_argument('--id', type=str, default=None,
                        help='''unique identifier of this client (used for
                         naming output files and including in event message;
                         default=process ID)''')

    parser.add_argument('--delay', '-d', type=float, default=1,
                        help='''delay (in secs) before sending the event''')
    parser.add_argument('--quit_time', '-q', type=float, default=10,
                        help='''delay (in secs) before quitting and recording statistics''')

    parser.add_argument('--port', '-p', type=int, default=9999,
                        help='''UDP port number to which data should be sent or received''')
    parser.add_argument('--address', '-a', type=str, default="127.0.0.1",
                        help='''IP address to which the data should be sent''')

    return parser.parse_args(args)


class SeismicClient(asyncore.dispatcher):

    def __init__(self, config):
        # super(SeismicClient, self).__init__()
        asyncore.dispatcher.__init__(self)

        # store configuration options and validate them
        self.config = config
        if self.config.id is None:
            self.config.id = str(os.getpid())

        # self.my_ip = socket.gethostbyname(socket.gethostname())

        # Stores received UNIQUE events indexed by their 'id'
        # Includes the time they were received at
        self.events_rcvd = dict()

        # TODO: need to record time we started the quake somehow?

        # queue seismic event reporting
        from threading import Timer
        Timer(self.config.delay, self.send_event).start()

        # setup UDP network socket to listen for events on
        self.create_socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.bind(('', self.config.port))

        # record statistics after experiment finishes then clean up
        Timer(self.config.quit_time, self.finish).start()

    def send_event(self):
        event = dict()
        event['time_sent'] = time.time()
        event['id'] = self.config.id

        self.sendto(json.dumps(event), (self.config.address, self.config.port))

    def handle_read(self):
        """
        Receive an event and record the time we received it.
        If it's already been received, we simply record the fact that
        we've received a duplicate.
        """

        data = self.recv(4096)
        # ENHANCE: handle packets too large to fit in this buffer
        try:
            event = json.loads(data)
            print "received event %s" % event

            if event['id'] not in self.events_rcvd:
                event['time_rcvd'] = time.time()
                event['copies_rcvd'] = 1
                self.events_rcvd[event['id']] = event
            else:
                self.events_rcvd[event['id']]['copies_rcvd'] += 1

        except ValueError:
            print "Error parsing JSON from %s" % data

    def run(self):
        try:
            asyncore.loop()
        except:
            # seems as though this just crashes sometimes when told to quit
            return

    def finish(self):
        self.record_stats()
        self.close()

    def record_stats(self):
        """Records the received picks for consumption by another script
        that will analyze the resulting performance."""

        fname = "_".join([self.config.file, self.config.id])
        with open(fname, "w") as f:
            f.write(json.dumps(self.events_rcvd))


if __name__ == '__main__':
    args = parse_args(sys.argv[1:])
    client = SeismicClient(args)
    client.run()
