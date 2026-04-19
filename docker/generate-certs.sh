#!/bin/bash
set -e

CERT_DIR="/etc/mosquitto/certs"

mkdir -p "$CERT_DIR"

cat > "$CERT_DIR/openssl.cnf" <<EOF
[req]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
x509_extensions = v3_ca

[dn]
CN = localhost

[v3_ca]
subjectAltName = DNS:localhost,DNS:mosquitto,IP:127.0.0.1
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = CA:true
EOF

openssl genrsa -out "$CERT_DIR/ca.key" 2048 2>/dev/null
openssl req -new -x509 -days 365 -key "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.crt" -config "$CERT_DIR/openssl.cnf" 2>/dev/null

cat > "$CERT_DIR/server.cnf" <<EOF
[req]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
req_extensions = v3_req

[dn]
CN = localhost

[v3_req]
subjectAltName = DNS:localhost,DNS:mosquitto,IP:127.0.0.1
EOF

openssl genrsa -out "$CERT_DIR/server.key" 2048 2>/dev/null
openssl req -new -key "$CERT_DIR/server.key" -out "$CERT_DIR/server.csr" -config "$CERT_DIR/server.cnf" 2>/dev/null
openssl x509 -req -days 365 -in "$CERT_DIR/server.csr" -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial -out "$CERT_DIR/server.crt" -extensions v3_req -extfile "$CERT_DIR/server.cnf" 2>/dev/null

chmod 644 "$CERT_DIR/ca.crt" "$CERT_DIR/server.crt"
chmod 600 "$CERT_DIR/ca.key" "$CERT_DIR/server.key"

rm -f "$CERT_DIR/server.csr" "$CERT_DIR/openssl.cnf" "$CERT_DIR/server.cnf" "$CERT_DIR/ca.srl"

echo "TLS certificates generated in $CERT_DIR"
openssl x509 -in "$CERT_DIR/server.crt" -text -noout 2>/dev/null | grep -A1 "Subject Alternative Name" || true
openssl x509 -in "$CERT_DIR/server.crt" -text -noout 2>/dev/null | grep "Subject Key Identifier" || true
