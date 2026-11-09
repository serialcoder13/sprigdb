#!/bin/sh
# The service account every role but the agent runs as.
set -e
getent group sprigdb >/dev/null || groupadd --system sprigdb
getent passwd sprigdb >/dev/null || useradd --system --gid sprigdb --home-dir /var/lib/sprigdb --shell /usr/sbin/nologin sprigdb
