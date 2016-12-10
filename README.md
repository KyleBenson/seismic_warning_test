# seismic_warning_test
Components for simulating a simplified seismic awareness scenario.  Clients report "picks", a server aggregates them,
 and clients receive notifications of which devices picked for localized situational awareness.

ASSUMPTIONS
-- Clients only send a single pick.  The server collects all picks during its buffering period in an array and sends them.


JSON schemas for events
-- At start of single event's lifetime:
{'id' : client_id,
'time_sent' : 123333434.3314,
-- When aggregated, the server puts them all in an array and adds some fields:
{'id' : 'aggregator',
'events' : [{event}, {event}, ....]
}
-- Upon receiving an event, the client adds some additional info:
{'time_rcvd' : 1320290202.232,
'copies_rcvd' : 3}
