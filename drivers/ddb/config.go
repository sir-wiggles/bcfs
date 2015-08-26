package ddb

import (
	log "github.com/Sirupsen/logrus"
	"github.com/fatih/structs"
	"program/backend"
)

type Config struct {
	Key    string
	Secret string
}

func (c *Config) LogConfigValues() {
	m := structs.Map(c)
	for k, v := range m {
		log.Infof("%s: %s", k, v)
	}
}

func (c *Config) GetBackendName() string {
	return PACKAGE_NAME
}

func (c *Config) InitializeBackend(graph backend.Graph) (backend.Graph, error) {
	return graph, nil
}