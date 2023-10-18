package oci

import (
	"github.com/restic/restic/internal/backend/test"
	"testing"
)

var configTests = []test.ConfigTestData[Config]{
	{S: "oci:ocitest", Cfg: Config{
		Bucket:      "ocitest",
		Prefix:      ".",
		Connections: 5,
	}},
	{S: "oci:ocitest/", Cfg: Config{
		Bucket:      "ocitest",
		Prefix:      ".",
		Connections: 5,
	}},
	{S: "oci:ocitest/prefix/directory", Cfg: Config{
		Bucket:      "ocitest",
		Prefix:      "prefix/directory",
		Connections: 5,
	}},
	{S: "oci:ocitest/prefix/directory/", Cfg: Config{
		Bucket:      "ocitest",
		Prefix:      "prefix/directory",
		Connections: 5,
	}},
}

func TestParseConfig(t *testing.T) {
	test.ParseConfigTester(t, ParseConfig, configTests)
}
