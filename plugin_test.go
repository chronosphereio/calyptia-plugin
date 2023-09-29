package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	f, err := os.Create(filepath.Join(t.TempDir(), "output.txt"))
	assert.NoError(t, err)

	defer func() {
		err := os.RemoveAll(f.Name())
		if err != nil {
			return
		}
	}()

	// Set permissions on the file to be world-writable
	err = os.Chmod(f.Name(), 0o777)
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

	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	go func() {
		for {
			if ctx.Err() != nil {
				return
			}

			contents, err := io.ReadAll(f)
			assert.NoError(t, err)

			contents = bytes.TrimSpace(contents)
			lines := strings.Split(string(contents), "\n")

			for _, line := range lines {
				if line == "" {
					continue
				}

				var got struct {
					SkipMe               bool     `json:"skip_me"`
					Foo                  string   `json:"foo"`
					Message              string   `json:"message"`
					TmplOut              string   `json:"tmpl_out"`
					MultilineSplit       []string `json:"multiline_split"`
					TookToSend10Messages string   `json:"took_to_send_10_messages"`
				}

				err := json.Unmarshal([]byte(line), &got)
				assert.NoError(t, err)

				if got.SkipMe {
					continue
				}

				tookToSend10Messages, err := time.ParseDuration(got.TookToSend10Messages)
				assert.NoError(t, err)

				assert.True(t, tookToSend10Messages < time.Millisecond*10)

				assert.Equal(t, "bar", got.Foo)
				assert.Equal(t, "hello from go-test-input-plugin", got.Message)
				assert.Equal(t, "inside double quotes\nnew line", got.TmplOut)
				assert.Equal(t, []string{"foo", "bar"}, got.MultilineSplit)

				t.Logf("took %s", time.Since(start))

				cancel()
				return
			}
		}
	}()

	<-ctx.Done()

	err = pool.Client.StopContainer(fbit.ID, 6)
	assert.NoError(t, err)

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("timeout exceeded")
	}
}
