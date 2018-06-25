# PhoenixdbWrapper
A simple wrapper of phoenixdb python lib with two feature.

1. Object based Load Balancing support

By adding an http header "clientid", which is and uuid, 
invert proxy(nginx is my choice) can treat this "clientid" as
a session id to apply "stick session".

2. Auto alter table by adding new columns

Simply try to add necessary column definitions and retry the 
query when needed.
