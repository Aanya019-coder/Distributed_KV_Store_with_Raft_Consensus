#!/usr/bin/env bash
set -euo pipefail

# Create certs directory
mkdir -p certs
cd certs

# 1. Generate CA key and self-signed certificate
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -subj "/CN=Raft-CA" -days 365 -out ca.pem

# 2. Generate Node/Peer key and CSR
openssl genrsa -out node.key 2048
openssl req -new -key node.key -subj "/CN=localhost" -out node.csr

# 3. Create a configuration file for SANs
cat <<EOF > node.ext
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

# 4. Sign Node certificate using CA certificate
openssl x509 -req -in node.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out node.pem -days 365 -sha256 -extfile node.ext

# Clean up CSR and extension file
rm node.csr node.ext
echo "mTLS certificates generated successfully in certs/ directory."
