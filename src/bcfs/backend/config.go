package backend

import "fmt"

// An interface to allow different drivers to have their own unique config.
type Config map[string]interface{}

func (c Config) StringKey(key string) string {
	if val, ok := c[key]; ok {
		switch vv := val.(type) {
		case string:
			return vv
		default:
			panic(fmt.Errorf("Invalid %s parameter type from config: %T", key, val))
		}
	}
	panic(fmt.Errorf("No such key: %s", key))
}

func (c Config) IntKey(key string) int {
	if val, ok := c[key]; ok {
		switch vv := val.(type) {
		case int:
			return vv
		case int64:
			return int(vv)
		default:
			panic(fmt.Errorf("Invalid %s parameter type from config: %T", key, val))
		}
	}
	panic(fmt.Errorf("No such key: %s", key))
}
