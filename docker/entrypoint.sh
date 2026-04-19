#!/bin/bash
set -e

/usr/local/bin/generate-certs.sh
/usr/local/bin/setup-auth.sh

chown mosquitto:mosquitto /etc/mosquitto/certs/server.key /etc/mosquitto/certs/server.crt /etc/mosquitto/certs/ca.crt
chmod 640 /etc/mosquitto/certs/server.key
chown mosquitto:mosquitto /etc/mosquitto/passwd
chmod 640 /etc/mosquitto/passwd

echo "Starting Mosquitto broker..."
mosquitto -c /etc/mosquitto/mosquitto.conf -d
sleep 1

PID=$(pgrep -x mosquitto || echo "unknown")
echo "Mosquitto broker is running (PID: $PID)"
echo "  Plain MQTT:   localhost:1883 (anonymous)"
echo "  TLS+Auth MQTTS: localhost:8883 (user: pipepp_test, cert: /etc/mosquitto/certs/ca.crt)"

exec "$@"
