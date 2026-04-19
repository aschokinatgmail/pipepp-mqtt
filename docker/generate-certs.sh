#!/bin/bash
set -e

CERT_DIR=/etc/mosquitto/certs
mkdir -p "$CERT_DIR"

openssl genrsa -out "$CERT_DIR/ca.key" 2048 2>/dev/null
openssl req -new -x509 -days 3650 \
    -key "$CERT_DIR/ca.key" \
    -out "$CERT_DIR/ca.crt" \
    -subj "/CN=pipepp-test-ca" 2>/dev/null

openssl genrsa -out "$CERT_DIR/server.key" 2048 2>/dev/null

cat > "$CERT_DIR/server-ext.cnf" << 'EOF'
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost,DNS:mosquitto,IP:127.0.0.1
EOF

openssl req -new \
    -key "$CERT_DIR/server.key" \
    -out "$CERT_DIR/server.csr" \
    -subj "/CN=localhost" 2>/dev/null

openssl x509 -req -days 3650 \
    -in "$CERT_DIR/server.csr" \
    -CA "$CERT_DIR/ca.crt" \
    -CAkey "$CERT_DIR/ca.key" \
    -CAcreateserial \
    -out "$CERT_DIR/server.crt" \
    -extfile "$CERT_DIR/server-ext.cnf" 2>/dev/null

chmod 644 "$CERT_DIR/ca.crt" "$CERT_DIR/server.crt"
chmod 600 "$CERT_DIR/server.key" "$CERT_DIR/ca.key"
chown mosquitto:mosquitto "$CERT_DIR/server.key"

echo "TLS certificates generated in $CERT_DIR"
openssl x509 -in "$CERT_DIR/server.crt" -noout -text 2>/dev/null | grep -A2 "Subject Alternative Name" || true
