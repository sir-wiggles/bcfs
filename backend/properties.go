package backend

import (
	"fmt"
)

type PropertyType int

const (
	StringProperty PropertyType = iota
	NumberProperty
	BinaryProperty
)

type Property struct {
	Type  PropertyType
	Value interface{}
}

// Properties holdes the values of a node
type Properties map[string]*Property

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
	if property, ok := p[key]; ok {
		switch property.Type {
		case StringProperty:
			return property.Value.(string), nil
		default:
			return "", fmt.Errorf("Invalid %s parameter type: %T", key, property)
		}
	}
	return "", fmt.Errorf("No such key: %s", key)
}

// GetInt pulls an int out of Properties
func (p Properties) GetInt(key string) (int, error) {
	if property, ok := p[key]; ok {
		switch property.Type {
		case NumberProperty:
			return property.Value.(int), nil
		default:
			return 0, fmt.Errorf("Invalid %s parameter type: %T", key, property)
		}
	}
	return 0, fmt.Errorf("No such key: %s", key)
}

func (p *Properties) SetKey(key string, value interface{}) {
	(*p)[key] = &Property{StringProperty, value}
}

func (p *Properties) SetString(key string, value string) {
	(*p)[key] = &Property{StringProperty, value}
}

func (p *Properties) SetNumber(key string, value string) {
	(*p)[key] = &Property{NumberProperty, value}
}

func (p *Properties) SetBinary(key string, value []byte) {
	(*p)[key] = &Property{BinaryProperty, value}
}
