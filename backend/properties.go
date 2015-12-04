package backend

import (
	"fmt"
)

// Properties holdes the values of a node
type Properties map[string]interface{}

type Propertyers interface {
	GetString(string) (string, error)
	GetInt(string) (int, error)
	SetKey(string, interface{})
	SetString(string, string)
	SetNumber(string, string)
	SetBinary(string, []byte)
}

// GetString pulls a string type out of Properties
func (p Properties) GetString(key string) (string, error) {
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

// GetInt pulls an int out of Properties
func (p Properties) GetInt(key string) (int, error) {
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

func (p *Properties) SetKey(key string, value interface{}) {
	(*p)[key] = value
}

func (p *Properties) SetString(key string, value string) {
	(*p)[key] = value
}

func (p *Properties) SetNumber(key string, value string) {
	(*p)[key] = value
}

func (p *Properties) SetBinary(key string, value []byte) {
	(*p)[key] = value
}
