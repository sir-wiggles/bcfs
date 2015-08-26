package neo

import (
	log "github.com/Sirupsen/logrus"
	"github.com/fatih/structs"
	"program/backend"
	"database/sql"
	_ "gopkg.in/cq.v1"
	"fmt"
)

type Config struct {
	User     string
	Password string
	Host     string
	Port     int64
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
	db, err := sql.Open("neo4j-cypher", c.getUrl())
	if err != nil {
		return nil, err
	}
	graph = NewDriver(db)
	return graph, nil
}

func (c *Config) getUrl() string {
	return fmt.Sprintf("http://%s:%s@%s:%d", c.User, c.Password, c.Host, c.Port)
}