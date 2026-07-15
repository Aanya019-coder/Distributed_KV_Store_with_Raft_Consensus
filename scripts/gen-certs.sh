#!/bin/sh
set -e

# Create directories
mkdir -p certs/node1 certs/node2 certs/node3

# Generate CA
openssl genrsa -out certs/ca.key 2048
openssl req -x509 -new -nodes -key certs/ca.key -subj "/CN=Raft-CA" -days 365 -out certs/ca.pem

# Generate Node Key & CSR
openssl genrsa -out certs/node.key 2048
openssl req -new -key certs/node.key -subj "/CN=localhost" -out certs/node.csr

# Create Extension Config including Docker container hostnames
cat <<EOF > certs/node.ext
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = node1
DNS.3 = node2
DNS.4 = node3
IP.1 = 127.0.0.1
EOF

# Sign Certificate
openssl x509 -req -in certs/node.csr -CA certs/ca.pem -CAkey certs/ca.key -CAcreateserial -out certs/node.pem -days 365 -sha256 -extfile certs/node.ext

# Distribute to node folders
for i in 1 2 3; do
    cp certs/ca.pem certs/node$i/ca.pem
    cp certs/node.pem certs/node$i/node.pem
    cp certs/node.key certs/node$i/node.key
done

# Cleanup root files
rm -f certs/node.csr certs/node.ext certs/ca.srl certs/ca.key certs/node.key certs/node.pem certs/ca.pem

echo "mTLS certificates generated and distributed successfully."

