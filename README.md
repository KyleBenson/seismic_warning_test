# seismic_warning_test
RIDE-enabled SCALE Client components for simulating a simplified seismic awareness scenario. Clients report "picks", a server aggregates them, and clients receive notifications of which devices picked for localized situational awareness.

## Getting started

Clone the [RIDE repo](https://github.com/KyleBenson/ride) and follow directions for how to set it up with the Mininet-based experiments, which this repo was designed to support.

You'll need to enable access to the RIDE Python package.  I do this by simply putting a symbolic link in this directory that points to the package.  If your PYTHONPATH includes '.', this should allow the modules to `import ride`.

## Using the seismic_alert SCALE modules

Run the clients all at the same time configured to send their data to a particular location.  This could be a multicast
address or it could be the server.  If the server, it will aggregate data and then send it to its specified IP address.
Recommended that you set the destination of this aggregated data to be a multicast address or else use
e.g. OVS with group tables to forward the message to all the subscribing clients.  You could easily extend the server
to do this manually but we didn't want to add the extra out-of-band configuration.

After running the experiment (probably using Mininet), put the output files into a single directory.  When you have
several experiments that you want to compare, run the statistics.py file on those directories to see the results
parsed using Pandas.  We loaded them into non-Python-based plotting systems afterwards, but you could easily extend `statistics.py` to immediately plot them using matplotlib.

ASSUMPTIONS
-- Clients repeatedly send picks once the earthquake happens until it quits.
-- The aggregation server collects all picks during its buffering period in a dict and sends them in an array as just the publisher IP address and earthquake *sequence number*.
-- The Aggregator keeps sending all of the picks received to date each buffering period.


## TODO
* Document JSON schemas for events as they move from pick --> alert --> subscriber receiving the alert
* Similarly, document `statistics.py` and what exactly that data format represents
* Potentially just merge this repo back into the main RIDE repo?  The point is that this one is supposed to be the SCALE extension, but the main repo relies on it so it's probably better integrated directly back into RIDE...
