package configloader

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigLoader(t *testing.T) {
	tests := []struct {
		name           string
		fileName       string
		expectedConfig *CalyptiaConfig
		expectError    bool
	}{
		{
			name:     "Basic YAML config provided",
			fileName: "basic.yaml",
			expectedConfig: &CalyptiaConfig{
				URL:   "https://cloud-api.calyptia.com",
				Token: "token",
				TLS:   true,
			},
			expectError: false,
		},
		{
			name:     "Basic YAML no TLS config provided",
			fileName: "basic-no-tls.yaml",
			expectedConfig: &CalyptiaConfig{
				URL:   "http://cloud-api.calyptia.com",
				Token: "token",
				TLS:   false,
			},
			expectError: false,
		},
		{
			name:     "Basic INI config provided",
			fileName: "basic.ini",
			expectedConfig: &CalyptiaConfig{
				URL:   "https://cloud-api.calyptia.com",
				Token: "token",
				TLS:   true,
			},
			expectError: false,
		},
		{
			name:           "Incorrect YAML config provided",
			fileName:       "incorrect.yaml",
			expectedConfig: nil,
			expectError:    true,
		},
		{
			name:           "Incorrect INI config provided",
			fileName:       "incorrect.ini",
			expectedConfig: nil,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loader := NewDefaultLoader()
			config := loader.LoadFromFiles(filepath.Join("testdata", tt.fileName))
			if tt.expectError {
				assert.Nil(t, config)
			} else {
				assert.Equal(t, tt.expectedConfig, config)
			}
		})
	}
}
