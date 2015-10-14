package backend

import (
	"fmt"
)

// representation of a node id which is a 32 hex char string
type id string

// The key value struct for nodes
type Properties map[string]interface{}

// helper method to get a string out of properties
func (p Properties) StringKey(key string) (string, error) {
	if val, ok := p[key]; ok {
		switch vv := val.(type) {
		case string:
			return vv, nil
		default:
			return "", fmt.Errorf("Invalid %s parameter type: %T", key, val)
		}
	}
	return "", fmt.Errorf("No such key: %s", key)
}

// helper method to get an int out of properties
func (p Properties) IntKey(key string) (int, error) {
	if val, ok := p[key]; ok {
		switch vv := val.(type) {
		case int64:
			return int(vv), nil
		default:
			return 0, fmt.Errorf("Invalid %s parameter type: %T", key, val)
		}
	}
	return 0, fmt.Errorf("No such key: %s", key)
}
