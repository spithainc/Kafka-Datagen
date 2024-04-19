package src

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/jcmturner/gokrb5/v8/client"
	krbConfig "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"golang.org/x/oauth2/clientcredentials"
)

/**********************************************************************
**                                                                   **
**                        SASL, SSL setting                          **
**                                                                   **
***********************************************************************/
func auth(opts []kgo.Opt) ([]kgo.Opt, error) {
	/**
	 * SASL Value Settings
	 */
	var (
		PLAIN                      = "PLAIN"
		SCRAM_SHA_256              = "SCRAM-SHA-256"
		SCRAM_SHA_512              = "SCRAM-SHA-512"
		OAUTHBEARER                = "OAUTHBEARER"
		GSSAPI                     = "GSSAPI"
		AWS_MSK_IAM                = "AWS_MSK_IAM"
		DEFAULT_TLS_TIMEOUT_SECOND = 15
	)

	/**
	 * SASL Config Settings
	 */
	if Module.Producer.Sasl.Mechanism == AWS_MSK_IAM {
		// AWS_MSK_IAM
		opts = append(opts, kgo.SASL(aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
			return aws.Auth{
				AccessKey: Module.Producer.Sasl.AwsAccessKeyId,
				SecretKey: Module.Producer.Sasl.AwsSecretAccessKey,
				UserAgent: "franz-go/creds_test/v1.0.0",
			}, nil
		})))
		opts = append(opts, kgo.Dialer((&tls.Dialer{NetDialer: &net.Dialer{Timeout: time.Second * time.Duration(DEFAULT_TLS_TIMEOUT_SECOND)}}).DialContext))
	} else if Module.Producer.Sasl.Mechanism == PLAIN {
		// PLAIN
		opts = append(opts, kgo.SASL(plain.Auth{
			User: Module.Producer.Sasl.Username,
			Pass: Module.Producer.Sasl.Password,
		}.AsMechanism()))
	} else if Module.Producer.Sasl.Mechanism == SCRAM_SHA_256 {
		// SCRAM-SHA-256
		opts = append(opts, kgo.SASL(scram.Auth{
			User: Module.Producer.Sasl.Username,
			Pass: Module.Producer.Sasl.Password,
		}.AsSha256Mechanism()))
	} else if Module.Producer.Sasl.Mechanism == SCRAM_SHA_512 {
		// SCRAM-SHA-512
		opts = append(opts, kgo.SASL(scram.Auth{
			User: Module.Producer.Sasl.Username,
			Pass: Module.Producer.Sasl.Password,
		}.AsSha512Mechanism()))
	} else if Module.Producer.Sasl.Mechanism == OAUTHBEARER {
		// OAUTHBEARER
		o2Config := &clientcredentials.Config{
			ClientID:     Module.Producer.Sasl.ClientId,
			ClientSecret: Module.Producer.Sasl.ClientSecret,
			TokenURL:     Module.Producer.Sasl.TokenEndpoint,
		}
		// get AccessToken from TokenEndpoint
		token, err := o2Config.Token(context.Background())
		if err != nil {
			Log.Error(fmt.Sprintln("failed to get token :", err))
			return nil, err
		}
		opts = append(opts, kgo.SASL(oauth.Auth{
			Token: token.AccessToken,
		}.AsMechanism()))
	} else if Module.Producer.Sasl.Mechanism == GSSAPI {
		// GSSAPI
		krbClient, err := getKerberosClient(Module.Producer.Sasl.KerberosConfig, Module.Producer.Sasl.KeyTab, Module.Producer.Sasl.Username, Module.Producer.Sasl.Realm)
		if err != nil {
			Log.Error(fmt.Sprintln("failed to get Kerberos Client :", err))
			return nil, err
		}
		opts = append(opts, kgo.SASL(kerberos.Auth{
			Client:  krbClient,
			Service: Module.Producer.Sasl.Servicename,
		}.AsMechanism()))
	}

	/**
	 * SSL Setting
	 */
	var emptyModule ConfigModule
	if Module.Producer.Tls != emptyModule.Producer.Tls {
		var tlsConfig *tls.Config
		var err error
		if Module.Producer.Tls.Certfile == "" || Module.Producer.Tls.Keyfile == "" || Module.Producer.Tls.Cafile != "" {
			// no mutual
			tlsConfig, err = newTLSConfigOnlyCa(Module.Producer.Tls.Cafile, Module.Producer.Tls.SkipVerify)
			if err != nil {
				Log.Error(fmt.Sprintln(err))
				return nil, err
			}
		} else if Module.Producer.Tls.Certfile != "" || Module.Producer.Tls.Keyfile != "" || Module.Producer.Tls.Cafile != "" {
			// mutual
			tlsConfig, err = newTLSConfig(Module.Producer.Tls.Certfile, Module.Producer.Tls.Keyfile, Module.Producer.Tls.Cafile, Module.Producer.Tls.SkipVerify)
			if err != nil {
				Log.Error(fmt.Sprintln(err))
				return nil, err
			}
		} else {
			Log.Error(fmt.Sprintln("Tls setting is not recongnized"))
			return nil, err
		}
		tlsDialer := &tls.Dialer{
			NetDialer: &net.Dialer{Timeout: time.Second * time.Duration(DEFAULT_TLS_TIMEOUT_SECOND)},
			Config:    tlsConfig,
		}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}
	return opts, nil
}

/**********************************************************************
**                                                                   **
**                         Kerberos support                          **
**                                                                   **
***********************************************************************/
// GSSAPI
func getKerberosClient(configPath string, keyTabPath string, username string, realm string) (*client.Client, error) {
	// Kerberos 클라이언트 구성 생성
	kbConf, err := krbConfig.Load(configPath)
	if err != nil {
		return nil, err
	}

	// Keytab 파일 로드
	kbTab, err := keytab.Load(keyTabPath)
	if err != nil {
		return nil, err
	}

	krbClient := client.NewWithKeytab(username, realm, kbTab, kbConf)

	err = krbClient.Login()
	if err != nil {
		return nil, err
	}

	return krbClient, err
}

/**********************************************************************
**                                                                   **
**                            Mutual Tls                             **
**                                                                   **
***********************************************************************/
func newTLSConfig(clientCertFile, clientKeyFile, caCertFile string, insecureSkipVerify bool) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		cert = tls.Certificate{}
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	caCert, err := os.ReadFile(caCertFile)
	if err != nil {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
}

/**********************************************************************
**                                                                   **
**                            Tls only CA                            **
**                                                                   **
***********************************************************************/
func newTLSConfigOnlyCa(caCertFile string, insecureSkipVerify bool) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load CA cert
	caCert, err := os.ReadFile(caCertFile)
	if err != nil {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.BuildNameToCertificate()
	tlsConfig.InsecureSkipVerify = insecureSkipVerify
	return &tlsConfig, err
}
