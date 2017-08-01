#!/bin/sh

systemctl daemon-reload 2> /dev/null || true

# fix broken chmod in release 0.10 packed by travis-ci
chmod 644 /etc/logrotate.d/go-carbon || true
chmod 644 /lib/systemd/system/go-carbon.service || true
chmod 755 /etc/init.d/go-carbon || true
chmod 644 /etc/go-carbon/go-carbon.conf || true
chmod 644 /etc/go-carbon/storage-schemas.conf || true
chmod 644 /etc/go-carbon/storage-aggregation.conf || true
