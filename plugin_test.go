package plugin

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
)

func TestPlugin(t *testing.T) {
	flag.Parse()

	pool, err := dockertest.NewPool("")
	assert.NoError(t, err)
	testPlugin(t, pool)
}

func testPlugin(t *testing.T, pool *dockertest.Pool) {
	auths, err := dc.NewAuthConfigurationsFromDockerCfg()
	assert.NoError(t, err)

	pwd, err := os.Getwd()
	assert.NoError(t, err)

	//nolint:gosec //required filesystem access to read fixture data.
	f, err := os.Create(filepath.Join(pwd, "testdata", "output.txt"))
	assert.NoError(t, err)

	t.Cleanup(func() { _ = f.Close() })
	t.Cleanup(func() { _ = f.Truncate(0) })

	err = f.Truncate(0)
	assert.NoError(t, err)

	buildOpts := dc.BuildImageOptions{
		Name:         "fluent-bit-go.localhost",
		ContextDir:   ".",
		Dockerfile:   "./testdata/Dockerfile",
		Platform:     "linux/amd64",
		OutputStream: io.Discard,
		ErrorStream:  io.Discard,
		Pull:         true,
		AuthConfigs:  *auths,
	}

	if testing.Verbose() {
		buildOpts.OutputStream = os.Stdout
		buildOpts.ErrorStream = os.Stderr
	}

	err = pool.Client.BuildImage(buildOpts)
	assert.NoError(t, err)

	fbit, err := pool.Client.CreateContainer(dc.CreateContainerOptions{
		Config: &dc.Config{
			Image: "fluent-bit-go.localhost",
		},
		HostConfig: &dc.HostConfig{
			AutoRemove: true,
			Mounts: []dc.HostMount{
				{
					Source: filepath.Join(pwd, "testdata", "fluent-bit.conf"),
					Target: "/fluent-bit/etc/fluent-bit.conf",
					Type:   "bind",
				},
				{
					Source: filepath.Join(pwd, "testdata", "plugins.conf"),
					Target: "/fluent-bit/etc/plugins.conf",
					Type:   "bind",
				},
				{
					Source: f.Name(),
					Target: "/fluent-bit/etc/output.txt",
					Type:   "bind",
				},
			},
		},
	})
	assert.NoError(t, err)

	t.Cleanup(func() {
		_ = pool.Client.RemoveContainer(dc.RemoveContainerOptions{
			ID: fbit.ID,
		})
	})

	go func() {
		if testing.Verbose() {
			_ = pool.Client.Logs(dc.LogsOptions{
				Container:    fbit.ID,
				OutputStream: os.Stdout,
				ErrorStream:  os.Stderr,
				Stdout:       true,
				Stderr:       true,
				Follow:       true,
			})
		}
	}()

	err = pool.Client.StartContainer(fbit.ID, nil)
	assert.NoError(t, err)

	time.Sleep(time.Second * 30)

	err = pool.Client.StopContainer(fbit.ID, 5)
	assert.NoError(t, err)

	contents, err := io.ReadAll(f)
	assert.NoError(t, err)

	contents = bytes.TrimSpace(contents)
	lines := strings.Split(string(contents), "\n")

	// after 5 seconds of fluentbit running, there should be at least 1 record.
	if d := len(lines); d < 1 || (d == 1 && lines[0] == "") {
		t.Fatal("expected at least 1 lines")
	}

	for _, line := range lines {
		if line == "" {
			t.Log("skipping empty line")
			continue
		}

		var got struct {
			Foo     string `json:"foo"`
			Message string `json:"message"`
			Tmpl    string `json:"tmpl"`
		}

		err := json.Unmarshal([]byte(line), &got)
		assert.NoError(t, err)
		assert.Equal(t, "bar", got.Foo)
		assert.Equal(t, "hello from go-test-input-plugin", got.Message)
		assert.Equal(t, "inside double quotes\nnew line", got.Tmpl)
	}
}
