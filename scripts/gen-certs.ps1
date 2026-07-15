# Powershell script to generate mTLS certificates

$ErrorActionPreference = "Stop"

# Create certs directory
Write-Host "Creating certs directory..."
New-Item -ItemType Directory -Force -Path "certs" | Out-Null

$openssl = "C:\Program Files\Git\usr\bin\openssl.exe"

# 1. Generate CA key and self-signed certificate
Write-Host "Generating CA key and self-signed cert..."
& $openssl genrsa -out certs/ca.key 2048
& $openssl req -x509 -new -nodes -key certs/ca.key -subj "/CN=Raft-CA" -days 365 -out certs/ca.pem

# 2. Generate Node key and CSR
Write-Host "Generating Node key and CSR..."
& $openssl genrsa -out certs/node.key 2048
& $openssl req -new -key certs/node.key -subj "/CN=localhost" -out certs/node.csr

# 3. Create extension file for SANs
Write-Host "Creating extension configuration..."
$ext = @"
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
"@
Set-Content -Path certs/node.ext -Value $ext -Encoding Ascii

# 4. Sign Node certificate
Write-Host "Signing Node cert..."
& $openssl x509 -req -in certs/node.csr -CA certs/ca.pem -CAkey certs/ca.key -CAcreateserial -out certs/node.pem -days 365 -sha256 -extfile certs/node.ext

# Clean up
Remove-Item -Path certs/node.csr, certs/node.ext -Force -ErrorAction SilentlyContinue
Write-Host "mTLS certificates generated successfully in certs/ directory."
