#!/usr/bin/env bash

bin=influxdb-relay

GOOS=linux GOARCH=amd64 go build -o $bin main.go

# vultr-jp
  rsync -axz --progress $bin jp:/usr/local/bin/
  rsync -ax --progress influxdb-relay-vultrjp.toml jp:/usr/local/etc/influxdb-relay/conf.toml
  rsync -ax --progress ./scripts/influxdb-relay.service jp:/etc/systemd/system/
  ssh jp "systemctl restart influxdb-relay && systemctl status influxdb-relay"

 aly-jp
  rsync -axz --progress $bin jp2:/usr/local/bin/
  rsync -ax --progress influxdb-relay-alyjp.toml jp2:/usr/local/etc/influxdb-relay/conf.toml
  rsync -ax --progress ./scripts/influxdb-relay.service jp2:/etc/systemd/system/
  ssh jp2 "systemctl restart influxdb-relay &&  systemctl status influxdb-relay"
