#!/bin/bash
set -e

PASSWD_FILE=/etc/mosquitto/passwd
touch "$PASSWD_FILE"
mosquitto_passwd -b "$PASSWD_FILE" pipepp_test pipepp_test_pw
chmod 700 "$PASSWD_FILE"
chown mosquitto:mosquitto "$PASSWD_FILE"

echo "Password file created at $PASSWD_FILE"
