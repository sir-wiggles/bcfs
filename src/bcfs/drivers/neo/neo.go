package neo

import (
	"bcfs/backend"
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "gopkg.in/cq.v1"
)

type Driver struct {
	Connection *sql.DB
}

func newDriver(c *backend.Config) (backend.Graph, error) {

	url := fmt.Sprintf(
		"http://%s:%s@%s:%d",
		c.StringKey("user"),
		c.StringKey("password"),
		c.StringKey("host"),
		c.IntKey("port"),
	)

	db, err := sql.Open("neo4j-cypher", url)
	return &Driver{
		Connection: db,
	}, err
}

func init() {
	log.Infof("Registering %s as a backend", PACKAGE_NAME)
	backend.RegisterBackend(PACKAGE_NAME, newDriver)
}

func (d *Driver) Ping() error {
	err := d.Connection.Ping()
	return err
}

// Given a list of node ids return all the nodes and their properties
func (d *Driver) GetNodes(sid string, nids []string) (*backend.Nodes, error) {
	return nil, nil
}
