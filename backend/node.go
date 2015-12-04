package backend

// Nodes maps a nid to it's properties. Used both in the request and the response
type Nodes map[string]*Properties

func (n *Nodes) GetNodeByID(id string) *Properties {
	node := (*n)[id]
	if node == nil {
		node = &Properties{}
		(*n)[id] = node
	}
	return node
}
