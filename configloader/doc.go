// Package configloader provides functionality to load Calyptia configurations
// from a set of specified configuration files. The main purpose of this package
// is to provide a simplified and streamlined way to fetch configurations without
// relying on environment variables.
//
// The package introduces a `Loader` type, which is responsible for loading and
// parsing the configuration files. It leverages the FileReader interface to read
// file contents, which allows for easy mocking in unit tests.
//
// Usage:
//
//	loader := configloader.NewLoader(nil) // uses the real file reader by default
//	config := loader.LoadFromFiles("path/to/config1.yaml", "path/to/config2.conf")
//
//	if config != nil {
//		fmt.Println("Loaded config:", config)
//	}
//
// The package also supports multiple file formats, including `.conf`, `.ini`, `.yaml`,
// and `.yml`. The first valid Calyptia configuration found among the provided files
// will be returned. If none is found, the function returns nil.
package configloader
