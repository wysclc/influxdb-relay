#!/usr/bin/env bash

bin=influxdb-relay

GOOS=linux GOARCH=amd64 go build -o $bin main.go

rsync -avx --progress $bin hk01:/usr/bin/
rsync -avx --progress influxdb-relay.conf hk01:/etc/influxdb-relay/
rsync -avx --progress ./scripts/influxdb-relay.service hk01:/etc/systemd/system/
ssh hk01 "systemctl stop influxdb-relay && \
 systemctl start influxdb-relay && \
 systemctl status influxdb-relay"