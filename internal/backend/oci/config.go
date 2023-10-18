package oci

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/options"
	"github.com/restic/restic/internal/restic"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/common/auth"
)

func init() {
	options.Register("oci", Config{})
}

const (
	OCI_AUTH_INSTANCE_PRINCIPAL_ENV_VAR = "OCI_CLI_AUTH"
	OCI_REGION_ENV_VAR                  = "OCI_REGION"
	OCI_USER_ENV_VAR                    = "OCI_USER"
	OCI_FINGERPRINT_ENV_VAR             = "OCI_FINGERPRINT"
	OCI_KEY_FILE_ENV_VAR                = "OCI_KEY_FILE"
	OCI_TENANCY_ENV_VAR                 = "OCI_TENANCY"
	OCI_PASSPHRASE_ENV_VAR              = "OCI_PASSPHRASE"
	OCI_COMPARTMENT_ENV_VAR             = "OCI_COMPARTMENT_OCID"
	InstnacePrincipalKey                = "instance_principal"
)

// Config holds the configuration required for communicating with the OCI
type Config struct {
	Region                string
	TenancyID             string
	UserID                string
	PrivateKeyFile        string
	PrivateKey            options.SecretString
	Fingerprint           string
	Passphrase            string
	UseInstancePrincipals bool
	Bucket                string
	Prefix                string
	CompartmentOCID       string
	Connections           uint   `option:"connections" help:"set a limit for the number of concurrent connections (default: 5)"`
	Layout                string `option:"layout" help:"use this backend layout (default: auto-detect)"`
}

// NewConfig returns a new Config with the default values filled in.
func NewConfig() Config {
	return Config{
		Connections: 5,
	}
}

// ParseConfig parses the string s and extracts the OCI config. The two
// supported configuration formats are oci://host/bucketname/prefix and
// oci:host/bucketname/prefix. If no prefix is given the prefix "restic" will be used.
// valid formats
// "oci:bucket-name"
// "oci:bucket-name/test1"
// "oci:bucket-name/test1/test2"
func ParseConfig(s string) (*Config, error) {
	if !strings.HasPrefix(s, "oci:") {
		return nil, errors.New("oci: invalid format")
	}

	// strip prefix "oci:"
	s = s[4:]

	// use the first entry of the path as the bucket name and the
	// remainder as prefix
	bucketName, prefix, _ := strings.Cut(s, "/")
	prefix = strings.TrimPrefix(path.Clean(prefix), "/")
	cfg := NewConfig()
	cfg.Bucket = bucketName
	cfg.Prefix = prefix
	return &cfg, nil

}

var _ restic.ApplyEnvironmenter = &Config{}

// ApplyEnvironment saves values from the environment to the config.
func (cfg *Config) ApplyEnvironment(prefix string) {

	var instancePrincipal bool
	if getEnvValuesWithDefault(OCI_AUTH_INSTANCE_PRINCIPAL_ENV_VAR, "user_principal") == InstnacePrincipalKey {
		instancePrincipal = true
	}
	cfg.UseInstancePrincipals = instancePrincipal

	if cfg.Region == "" {
		cfg.Region = os.Getenv(prefix + OCI_REGION_ENV_VAR)
	}

	if cfg.CompartmentOCID == "" {
		cfg.CompartmentOCID = os.Getenv(prefix + OCI_COMPARTMENT_ENV_VAR)
	}

	if !cfg.UseInstancePrincipals {
		if cfg.TenancyID == "" {
			cfg.TenancyID = os.Getenv(prefix + OCI_TENANCY_ENV_VAR)
		}
		if cfg.UserID == "" {
			cfg.UserID = os.Getenv(prefix + OCI_USER_ENV_VAR)
		}
		if cfg.Fingerprint == "" {
			cfg.Fingerprint = os.Getenv(prefix + OCI_FINGERPRINT_ENV_VAR)
		}
		if cfg.PrivateKeyFile == "" {
			cfg.PrivateKeyFile = os.Getenv(prefix + OCI_KEY_FILE_ENV_VAR)
		}

		_, err := os.Stat(filepath.Clean(cfg.PrivateKeyFile))
		if err != nil {
			fmt.Println("Unable to read private key file for provider OCI: OCI_KEY_FILE")
			os.Exit(1)
		}

		keyData, err := os.ReadFile(filepath.Clean(cfg.PrivateKeyFile))
		if err != nil {
			fmt.Println("Unable to read private key data for provider OCI: OCI_KEY_FILE")
			os.Exit(1)
		}
		cfg.PrivateKey = options.NewSecretString(string(keyData))

		if cfg.Passphrase == "" {
			cfg.Passphrase = os.Getenv(prefix + OCI_PASSPHRASE_ENV_VAR)
		}
	}
	spew.Dump(cfg)

}

// NewConfigurationProvider build the OCI Auth provider
func NewConfigurationProvider(cfg *Config) (common.ConfigurationProvider, error) {
	if cfg.UseInstancePrincipals {
		return auth.InstancePrincipalConfigurationProvider()
	}
	return NewConfigurationProviderWithUserPrincipal(cfg)
}

// NewConfigurationProviderWithUserPrincipal build the OCI Auth provider with user principal data
func NewConfigurationProviderWithUserPrincipal(cfg *Config) (common.ConfigurationProvider, error) {
	var conf common.ConfigurationProvider
	if cfg != nil {
		conf = common.NewRawConfigurationProvider(
			cfg.TenancyID,
			cfg.UserID,
			cfg.Region,
			cfg.Fingerprint,
			cfg.PrivateKey.Unwrap(),
			common.String(cfg.Passphrase))
		return conf, nil
	}
	return nil, nil
}

// getEnvValuesWithDefault utility function for getting env values with defaults if not set.
func getEnvValuesWithDefault(key, defaultValue string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		return defaultValue
	}
	return val
}
