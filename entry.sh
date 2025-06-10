#!/bin/sh

case "$1" in
    "server")
        exec /opt/practice-4/server
        ;;
    "db")
        exec /opt/practice-4/db
        ;;
    "balancer")
        shift
        exec /opt/practice-4/lb "$@"
        ;;
    *)
        echo "Usage: $0 {server|db|balancer}"
        exit 1
        ;;
esac