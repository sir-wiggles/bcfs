package backend

import (
	"fmt"
)

// Properties holdes the values of a node
type Properties map[string]interface{}

// StringKey pulls a string type out of Properties
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

// IntKey pulls an int out of Properties
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
