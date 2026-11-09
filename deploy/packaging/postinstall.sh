#!/bin/sh
# Units are installed, not enabled: the Ansible role enables each host's roles.
set -e
command -v systemctl >/dev/null && systemctl daemon-reload || true
