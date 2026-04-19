#!/bin/bash
set -e

PASSWD_FILE="/etc/mosquitto/passwd"

touch "$PASSWD_FILE"
mosquitto_passwd -b "$PASSWD_FILE" pipepp_test pipepp_test_pw 2>/dev/null || \
    mosquitto_passwd -c -b "$PASSWD_FILE" pipepp_test pipepp_test_pw 2>/dev/null

echo "Password file created at $PASSWD_FILE"
