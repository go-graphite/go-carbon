#!/bin/sh

/usr/bin/id carbon >/dev/null 2>&1 || /usr/sbin/useradd --system -U -s /bin/false -c "User for Graphite daemon" carbon
