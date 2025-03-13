// File: shared/certutil.go
package shared

import (
    "bytes"
    "crypto/ecdsa"
    "crypto/elliptic"
    "crypto/rand"
    "crypto/tls"
    "crypto/x509"
    "crypto/x509/pkix"
    "encoding/pem"
    "fmt"
    "math/big"
    "errors"
    "time"

    "strings"

    "github.com/hashicorp/go-hclog"
)

// CertificateConfig holds the configuration for generating TLS certificates
type CertificateConfig struct {
    CommonName  string
    ValidFor    time.Duration
    KeySize     int
    IsCA        bool
    ServerName  string
    DNSNames    []string
}

// DefaultCertificateConfig returns a default configuration for local development
func DefaultCertificateConfig() *CertificateConfig {
    return &CertificateConfig{
        CommonName:  "localhost",
        ValidFor:    24 * time.Hour,
        KeySize:     2048,
        IsCA:        true,
        ServerName:  "localhost",
        DNSNames:    []string{"localhost"},
    }
}

// GenerateCert generates a temporary certificate for plugin authentication.
// Returns the certificate and private key in PEM format.
func GenerateCert(logger hclog.Logger) ([]byte, []byte, error) {
    if logger == nil {
        logger = hclog.NewNullLogger()
    }

    logger.Debug("🔐 generating temporary certificate")

    // Generate ECDSA private key using P-521 curve
    key, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
    if err != nil {
        logger.Error("🔐❌ private key generation failed", "error", err)
        return nil, nil, err
    }
    logger.Debug("🔐✅ generated ECDSA P-521 private key")

    // Generate serial number
    serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
    serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
    if err != nil {
        logger.Error("🔐❌ serial number generation failed", "error", err)
        return nil, nil, err
    }

    logger.Debug("🔐✅ generated serial number", "serial", serialNumber)

    host := "localhost"
    template := &x509.Certificate{
        Subject: pkix.Name{
            CommonName:   host,
            Organization: []string{"HashiCorp"},
        },
        DNSNames: []string{host},
        ExtKeyUsage: []x509.ExtKeyUsage{
            x509.ExtKeyUsageClientAuth,
            x509.ExtKeyUsageServerAuth,
        },
        KeyUsage: x509.KeyUsageDigitalSignature |
            x509.KeyUsageKeyEncipherment |
            x509.KeyUsageKeyAgreement |
            x509.KeyUsageCertSign,
        BasicConstraintsValid: true,
        SerialNumber:         serialNumber,
        NotBefore:           time.Now().Add(-30 * time.Second),
        NotAfter:            time.Now().Add(262980 * time.Hour), // 30 years
        IsCA:                true,
    }

    serialBytes := template.SerialNumber.Bytes()
    serialHex := make([]string, len(serialBytes))
    for i, b := range serialBytes {
        serialHex[i] = fmt.Sprintf("%02x", b)
    }

    logger.Debug("   🔢 Serial Number: " + strings.Join(serialHex, ":"))

    logger.Debug("🔐📝 created certificate template",
        "common_name", template.Subject.CommonName,
        "organization", template.Subject.Organization,
        "dns_names", template.DNSNames)

    // Create self-signed certificate
    der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
    if err != nil {
        logger.Error("🔐❌ certificate creation failed", "error", err)
        return nil, nil, err
    }
    logger.Debug("🔐✅ created self-signed certificate")

    // PEM encode the certificate
    var certOut bytes.Buffer
    if err := pem.Encode(&certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
        logger.Error("🔐❌ certificate PEM encoding failed", "error", err)
        return nil, nil, err
    }

    // Marshal the private key
    keyBytes, err := x509.MarshalECPrivateKey(key)
    if err != nil {
        logger.Error("🔐❌ private key marshaling failed", "error", err)
        return nil, nil, err
    }

    // PEM encode the private key
    var keyOut bytes.Buffer
    if err := pem.Encode(&keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
        logger.Error("🔐❌ private key PEM encoding failed", "error", err)
        return nil, nil, err
    }

    logger.Debug("🔐✅ encoded certificate and private key as PEM")
    return certOut.Bytes(), keyOut.Bytes(), nil
}

// ParseCertificate parses a PEM encoded certificate and returns the x509 certificate
func ParseCertificate(certPEM []byte, logger hclog.Logger) (*x509.Certificate, error) {
    if logger == nil {
        logger = hclog.NewNullLogger()
    }

    logger.Debug("🔍 parsing PEM certificate")

    block, _ := pem.Decode(certPEM)
    if block == nil {
        logger.Error("🔍❌ failed to decode PEM block")
        return nil, fmt.Errorf("failed to decode PEM block")
    }

    cert, err := x509.ParseCertificate(block.Bytes)
    if err != nil {
        logger.Error("🔍❌ certificate parsing failed", "error", err)
        return nil, err
    }

    logger.Debug("🔍✅ certificate parsed successfully",
        "subject", cert.Subject.CommonName,
        "issuer", cert.Issuer.CommonName,
        "not_before", cert.NotBefore,
        "not_after", cert.NotAfter)

    return cert, nil
}

// ParsePrivateKey parses a PEM encoded ECDSA private key
func ParsePrivateKey(keyPEM []byte, logger hclog.Logger) (*ecdsa.PrivateKey, error) {
    if logger == nil {
        logger = hclog.NewNullLogger()
    }

    logger.Debug("🔍 parsing PEM private key")

    block, _ := pem.Decode(keyPEM)
    if block == nil {
        logger.Error("🔍❌ failed to decode PEM block")
        return nil, fmt.Errorf("failed to decode PEM block")
    }

    key, err := x509.ParseECPrivateKey(block.Bytes)
    if err != nil {
        logger.Error("🔍❌ private key parsing failed", "error", err)
        return nil, err
    }

    logger.Debug("🔍✅ private key parsed successfully")
    return key, nil
}

// CreateTLSConfig creates a TLS configuration suitable for client or server
func CreateTLSConfig(cert *x509.Certificate, key *ecdsa.PrivateKey, certPool *x509.CertPool, isServer bool, logger hclog.Logger) *tls.Config {
    if logger == nil {
        logger = hclog.NewNullLogger()
    }

    logger.Debug("🔒 creating TLS config", "is_server", isServer)

    config := &tls.Config{
        Certificates: []tls.Certificate{
            {
                Certificate: [][]byte{cert.Raw},
                PrivateKey:  key,
            },
        },
        MinVersion: tls.VersionTLS12,
    }

    if isServer {
        config.ClientAuth = tls.RequireAndVerifyClientCert
        config.ClientCAs = certPool
    } else {
        config.RootCAs = certPool
    }

    logger.Debug("🔒✅ TLS config created",
        "is_server", isServer,
        "min_version", "TLS1.2")

    return config
}

// generateCert generates a temporary certificate for plugin authentication. The
// certificate and private key are returns in PEM format.
func generateCert() (cert []byte, privateKey []byte, err error) {
    key, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
    if err != nil {
        return nil, nil, err
    }

    serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
    sn, err := rand.Int(rand.Reader, serialNumberLimit)
    if err != nil {
        return nil, nil, err
    }

    host := "localhost"

    template := &x509.Certificate{
        Subject: pkix.Name{
            CommonName:   host,
            Organization: []string{"HashiCorp"},
        },
        DNSNames: []string{host},
        ExtKeyUsage: []x509.ExtKeyUsage{
            x509.ExtKeyUsageClientAuth,
            x509.ExtKeyUsageServerAuth,
        },
        KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageKeyAgreement | x509.KeyUsageCertSign,
        BasicConstraintsValid: true,
        SerialNumber:          sn,
        NotBefore:             time.Now().Add(-30 * time.Second),
        NotAfter:              time.Now().Add(262980 * time.Hour),
        IsCA:                  true,
    }

    der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
    if err != nil {
        return nil, nil, err
    }

    var certOut bytes.Buffer
    if err := pem.Encode(&certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
        return nil, nil, err
    }

    keyBytes, err := x509.MarshalECPrivateKey(key)
    if err != nil {
        return nil, nil, err
    }

    var keyOut bytes.Buffer
    if err := pem.Encode(&keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
        return nil, nil, err
    }

    cert = certOut.Bytes()
    privateKey = keyOut.Bytes()

    return cert, privateKey, nil
}

// DecodeAndLogCertificate decodes a PEM-encoded certificate and logs its details.
//
// Parameters:
// - certPEM: the certificate in PEM format.
// - logger: an hclog.Logger for logging certificate details.
//
// Returns:
// - *x509.Certificate representing the parsed certificate.
// - error if decoding or parsing fails.
//func DecodeAndLogCertificate(certPEM string, logger hclog.Logger) (*x509.Certificate, error) {
func DecodeAndLogCertificate(certPEM string, logger hclog.Logger) error {
    block, _ := pem.Decode([]byte(certPEM))
    if block == nil {
        logger.Error("❌ Failed to decode certificate PEM")
        return errors.New("failed to decode certificate PEM")
    }

    cert, err := x509.ParseCertificate(block.Bytes)
    if err != nil {
        logger.Error("❌ Error parsing certificate: %v", err)
        return errors.New("Error parsing certificate.")
    }

    // Format serial number as colon-delimited hex
    serialBytes := cert.SerialNumber.Bytes()
    serialHex := make([]string, len(serialBytes))
    for i, b := range serialBytes {
        serialHex[i] = fmt.Sprintf("%02x", b)
    }

    logger.Debug("📜 Certificate Information:")
    logger.Debug("   🔢 Serial Number: " + strings.Join(serialHex, ":"))
    logger.Debug("   🏷️  Subject: " + cert.Subject.String())
    logger.Debug("   🏢 Organization: " + strings.Join(cert.Subject.Organization, ", "))
    logger.Debug("   🌐 Common Name: " + cert.Subject.CommonName)
    logger.Debug("   📆 Valid From: " + cert.NotBefore.String())
    logger.Debug("   📆 Valid To: " + cert.NotAfter.String())

    return nil
}
