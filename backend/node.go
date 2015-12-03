package backend

type Nodes map[string]*Properties

func (n *Nodes) GetNodeByID(id string) *Properties {
	return (*n)[id]
}
