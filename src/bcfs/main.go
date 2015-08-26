package main

import (
	"flag"

	"bcfs/backend"

	// Load all the knows drivers.  These drivers get registered in their init method call.
	//	"program/drivers/ddb"
	_ "bcfs/drivers/neo"

	log "github.com/Sirupsen/logrus"
	"github.com/fogcreek/mini"
)

type FilesystemConfig struct {
	BackendConfig *backend.Config
	LogLevel      log.Level
}

// Handles parsing the config file passed in from the commandline
func handleConfig(configFilename *string) (*FilesystemConfig, error) {
	cfg, err := mini.LoadConfiguration(*configFilename)
	if err != nil {
		log.Error(err.Error())
		log.Fatalf("Failed to parse the config file %s", *configFilename)
	}

	// Get the backend from the config file and panic if no backend is specified
	backendName := cfg.String("backend", "")
	if backendName == "" {
		log.Fatalf("Must specify a \"backend\" in the config file")
	}

	// Get the logging level from the config file and default to info if an invalid value is given.
	ll := cfg.String("log-level", "info")
	log.Infof("Setting log level to %s", ll)
	logLevel, err := log.ParseLevel(ll)
	if err != nil {
		log.Warnf("Invalid log level %s; defaulting to info", ll)
		logLevel = log.InfoLevel
	}

	// Get all the specific driver configuration from the config and populate the appropriate driver config,
	// that's based on the backend name.
	var backendConfig *backend.Config
	switch backendName {
	case "neo":
		backendConfig = &backend.Config{
			"name":     "neo",
			"user":     cfg.StringFromSection(backendName, "user", ""),
			"password": cfg.StringFromSection(backendName, "password", ""),
			"host":     cfg.StringFromSection(backendName, "host", ""),
			"port":     cfg.IntegerFromSection(backendName, "port", 7474),
		}
	}

	fcfg := &FilesystemConfig{
		BackendConfig: backendConfig,
		LogLevel:      logLevel,
	}

	return fcfg, nil
}

func main() {

	_cf := flag.String("c", "", "path to the config file")
	flag.Parse()
	cfg, err := handleConfig(_cf)
	if err != nil {
		log.Fatalf(err.Error())
	}

	log.SetLevel(cfg.LogLevel)

	driver, err := backend.GetBackend(cfg.BackendConfig)
	if err != nil {
		log.Fatalf(err.Error())
	}
	log.Debug(driver.Ping())
}
