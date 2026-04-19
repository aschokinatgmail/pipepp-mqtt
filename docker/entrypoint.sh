#!/bin/bash
set -e

/generate-certs.sh
/setup-auth.sh

echo "Starting Mosquitto broker..."
mosquitto -c /etc/mosquitto/mosquitto.conf -d
sleep 2

if pgrep -x mosquitto > /dev/null; then
    echo "Mosquitto broker is running (PID: $(pgrep -x mosquitto))"
    echo "  Plain MQTT:   localhost:1883 (anonymous)"
    echo "  TLS+Auth MQTTS: localhost:8883 (user: pipepp_test, cert: /etc/mosquitto/certs/ca.crt)"
else
    echo "ERROR: Mosquitto failed to start!"
    cat /var/log/mosquitto/mosquitto.log 2>/dev/null || true
    exit 1
fi

exec "$@"
