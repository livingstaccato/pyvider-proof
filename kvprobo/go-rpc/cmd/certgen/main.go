// File: cmd/certgen/main.go
package main

import (
    "bytes"
    "crypto/ecdsa"
    "crypto/elliptic"
    "crypto/rand"
    "crypto/x509"
    "crypto/x509/pkix"
    "encoding/pem"
    "fmt"
    "math/big"
    "time"
)

func generateCert() ([]byte, error) {
    key, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
    if err != nil {
        return nil, err
    }

    serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
    serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
    if err != nil {
        return nil, err
    }

    template := &x509.Certificate{
        Subject: pkix.Name{
            CommonName:   "localhost",
            Organization: []string{"HashiCorp"},
        },
        DNSNames: []string{"localhost"},
        ExtKeyUsage: []x509.ExtKeyUsage{
            x509.ExtKeyUsageClientAuth,
            x509.ExtKeyUsageServerAuth,
        },
        KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageKeyAgreement | x509.KeyUsageCertSign,
        BasicConstraintsValid: true,
        SerialNumber:          serialNumber,
        NotBefore:            time.Now().Add(-30 * time.Second),
        NotAfter:             time.Now().Add(262980 * time.Hour),
        IsCA:                 true,
    }

    der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
    if err != nil {
        return nil, err
    }

    var certOut bytes.Buffer
    if err := pem.Encode(&certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
        return nil, err
    }

    return certOut.Bytes(), nil
}

func main() {
    clientCert, err := generateCert()
    if err != nil {
        fmt.Printf("error generating client cert: %v\n", err)
        return
    }

    serverCert, err := generateCert()
    if err != nil {
        fmt.Printf("error generating server cert: %v\n", err)
        return
    }

    fmt.Printf("export PLUGIN_CLIENT_CERT='%s'\n", string(clientCert))
    fmt.Printf("export PLUGIN_SERVER_CERT='%s'\n", string(serverCert))
    fmt.Printf("export PLUGIN_PROTOCOL_VERSIONS='1'\n")
    fmt.Printf("export PLUGIN_TRANSPORTS='unix,tcp'\n")
    fmt.Printf("export BASIC_PLUGIN='hello'\n")
}
