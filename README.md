# Go Interview Exercise

### Generate HTTPS certificate

```bash
go run "$(go env GOROOT)\src\crypto\tls\generate_cert.go" --host localhost,127.0.0.1

