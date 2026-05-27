#!/usr/bin/env bash
set -euo pipefail

role="${1:-client}"
shift || true

case "$role" in
  namenode)
    if [[ ! -d /data/name/current ]]; then
      hdfs namenode -format -force -nonInteractive
    fi
    exec hdfs namenode
    ;;
  datanode)
    until nc -z namenode 9000; do sleep 1; done
    exec hdfs datanode
    ;;
  resourcemanager)
    exec yarn resourcemanager
    ;;
  nodemanager)
    until nc -z resourcemanager 8032; do sleep 1; done
    exec yarn nodemanager
    ;;
  historyserver)
    exec mapred historyserver
    ;;
  client)
    exec "$@"
    ;;
  *)
    exec "$role" "$@"
    ;;
esac
