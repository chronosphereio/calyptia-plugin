package configloader

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	fluentbitconfig "github.com/calyptia/go-fluentbit-config/v2"
)

// CalyptiaConfig contains configurations related to Calyptia.
type CalyptiaConfig struct {
	Token string
	URL   string
	TLS   bool
}

// FileReader is an interface for reading files.
type FileReader interface {
	ReadFile(filename string) ([]byte, error)
}

// LoaderInterface is an interface for the configuration loader.
type LoaderInterface interface {
	LoadFromFiles(configFiles ...string) *CalyptiaConfig
}

// RealFileReader is a concrete implementation of FileReader.
type RealFileReader struct{}

// ReadFile reads the content of the given filename.
func (r RealFileReader) ReadFile(filename string) ([]byte, error) {
	//nolint:gosec //required filesystem access to read fixture data.
	return os.ReadFile(filename)
}

// Loader is responsible for loading configurations.
type Loader struct {
	FileReader
}

// NewLoader initializes a new Loader with the given FileReader.
func NewLoader(fr FileReader) *Loader {
	if fr == nil {
		fr = RealFileReader{}
	}
	return &Loader{fr}
}

// parseConfig reads the content of the given config path and parses it into a fluentbit configuration object.
func (cl *Loader) parseConfig(path string) (*fluentbitconfig.Config, error) {
	// Read the content from the given path using the FileReader
	content, err := cl.FileReader.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading file %s: %v", path, err)
	}

	// Determine the format based on the file extension
	ext := filepath.Ext(path)
	var format fluentbitconfig.Format
	switch ext {
	case ".conf", ".ini":
		format = fluentbitconfig.FormatClassic
	case ".yaml", ".yml":
		format = fluentbitconfig.FormatYAML
	default:
		return nil, fmt.Errorf("unsupported file format for %s", path)
	}

	// Parse the content into a fluentbit configuration object
	parsedConfig, err := fluentbitconfig.ParseAs(string(content), format)
	if err != nil {
		return nil, fmt.Errorf("error parsing config from file %s: %v", path, err)
	}

	return &parsedConfig, nil
}

// LoadFromFiles loads the Calyptia configurations from the given list of files.
// It goes through each file, parses it, and attempts to retrieve the Calyptia configuration.
// The method returns the first valid Calyptia configuration it finds.
func (cl *Loader) LoadFromFiles(configFiles ...string) *CalyptiaConfig {
	calyptiaConfig := &CalyptiaConfig{
		TLS: false, // default value
	}

	for _, path := range configFiles {
		parsedConfig, err := cl.parseConfig(path)
		if err != nil {
			continue
		}

		// Attempt to find the calyptia plugin properties
		plugin, found := parsedConfig.Customs.FindByID("calyptia.0")
		if !found {
			continue
		}
		// Extract calyptia_tls and determine whether it's "true" or "on"
		value, exists := plugin.Properties.Get("calyptia_tls")
		if exists {
			tlsValue, ok := value.(string)
			if ok {
				tlsVal := strings.ToLower(tlsValue)
				calyptiaConfig.TLS = tlsVal == "true" || tlsVal == "on"
			} else {
				calyptiaConfig.TLS = false
			}
		}

		// Extract calyptia_host and construct full URL
		value, exists = plugin.Properties.Get("calyptia_host")
		if exists {
			host, ok := value.(string)
			if !ok {
				host = "" // Set to empty string if type assertion fails
			}
			if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
				if calyptiaConfig.TLS {
					host = "https://" + host
				} else {
					host = "http://" + host
				}
			}
			parsedURL, err := url.Parse(host)
			if err == nil {
				fullURL := parsedURL.Scheme + "://" + parsedURL.Hostname()
				if !(calyptiaConfig.TLS && parsedURL.Port() == "443" || !calyptiaConfig.TLS && parsedURL.Port() == "80") && parsedURL.Port() != "" {
					fullURL += ":" + parsedURL.Port()
				}
				calyptiaConfig.URL = fullURL
			}
		}

		// Extract api_key
		value, exists = plugin.Properties.Get("api_key")
		if exists {
			token, ok := value.(string)
			if !ok {
				token = "" // Set to empty string if type assertion fails
			}
			calyptiaConfig.Token = token
		}

		return calyptiaConfig
	}
	return nil
}

// NewDefaultLoader returns a new Loader instance with the default real file reader.
func NewDefaultLoader() *Loader {
	return &Loader{RealFileReader{}}
}
