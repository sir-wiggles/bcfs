package backend

// Nodes maps a nid to it's properties. Used both in the request and the response
type Nodes map[string]*Node

type Node struct {
	*Properties
}

func (n *Nodes) GetNodeByID(id string) *Node {
	node := (*n)[id]
	if node == nil {
		node = &Node{&Properties{}}
		(*n)[id] = node
	}
	return node
}
