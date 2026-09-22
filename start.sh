#!/usr/bin/env bash

bin=influxdb-relay
#hosts="jp jp2"
hosts="jp"

GOOS=linux GOARCH=amd64 go build -o $bin main.go

for host in $hosts;do
  rsync -avxz --progress $bin "$host":/usr/local/bin/
  rsync -avx --progress influxdb-relay.toml "$host":/usr/local/etc/influxdb-relay/conf.toml
  rsync -avx --progress ./scripts/influxdb-relay.service "$host":/etc/systemd/system/
  ssh "$host" "systemctl restart influxdb-relay && \
   systemctl status influxdb-relay"
done
