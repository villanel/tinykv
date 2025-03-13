package memdb

import (
	"encoding/base64"
	"encoding/json"
	"log"
	"strconv"

	"github.com/pingcap-incubator/tinykv/store/tikv"
)

type raftMap struct {
	client *tikv.RawKVClient
}
type DeepCopier interface {
	DeepCopy() interface{}
}

func NewRaftMap(client *tikv.RawKVClient) *raftMap {

	m := &raftMap{
		client: client,
	}
	// fill shards
	return m
}

type raftMapEntry struct {
	Type  string      `json:"Type"`
	Value interface{} `json:"Value"`
}

func (m *raftMap) Set(key string, value any) int {
	var typeStr string
	switch value.(type) {
	case *List:
		typeStr = "list"
	case Set:
		typeStr = "set"
	case nil:
		typeStr = "nil"
	case bool:
		typeStr = "bool"
	case int:
		typeStr = "int"
	case int8:
		typeStr = "int8"
	case int16:
		typeStr = "int16"
	case int32:
		typeStr = "int32"
	case int64:
		typeStr = "int64"
	case uint:
		typeStr = "uint"
	case uint8:
		typeStr = "uint8"
	case uint16:
		typeStr = "uint16"
	case uint32:
		typeStr = "uint32"
	case uint64:
		typeStr = "uint64"
	case float32:
		typeStr = "float32"
	case float64:
		typeStr = "float64"
	case string:
		typeStr = "string"
	case []byte: // 新增byte类型处理
		typeStr = "bytes"
	default:
		typeStr = "unknown"
	}

	entry := raftMapEntry{
		Type:  typeStr,
		Value: value,
	}
	jsonData, err := json.Marshal(entry)
	if err != nil {
		return 0
	}
	if err := m.client.Put([]byte(key), jsonData); err != nil {
		return 0
	}

	return 1
}

func (m *raftMap) Get(key string) (any, bool) {
	data, err := m.client.Get([]byte(key))
	if err != nil {
		log.Println(err)
		return nil, false
	}
	if data == nil {
		return nil, false
	}
	// if data == nil {
	// 	return nil, true
	// }
	var entry raftMapEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		log.Printf("JSON解码失败 | 数据:%s | 错误:%v",
			string(data), err)
		return nil, false
	}

	switch entry.Type {
	case "nil":
		return nil, true
	case "bool":
		if b, ok := entry.Value.(bool); ok {
			return b, true
		}

	case "int":
		return convertNumber(entry.Value,
			func(n json.Number) (any, error) { return n.Int64() },
			func(s string) (any, error) {
				v, err := strconv.Atoi(s)
				return v, err
			})
	case "int8":
		return convertNumber(entry.Value,
			func(n json.Number) (any, error) {
				v, err := n.Int64()
				return int8(v), err
			},
			func(s string) (any, error) {
				v, err := strconv.ParseInt(s, 10, 8)
				return int8(v), err
			})
	case "int16":
		return convertNumber(entry.Value,
			func(n json.Number) (any, error) {
				v, err := n.Int64()
				return int16(v), err
			},
			func(s string) (any, error) {
				v, err := strconv.ParseInt(s, 10, 16)
				return int16(v), err
			})
	case "int32":
		return convertNumber(entry.Value,
			func(n json.Number) (any, error) {
				v, err := n.Int64()
				return int32(v), err
			},
			func(s string) (any, error) {
				v, err := strconv.ParseInt(s, 10, 32)
				return int32(v), err
			})
	case "int64":
		return convertNumber(entry.Value,
			func(n json.Number) (any, error) {
				return n.Int64()
			},
			func(s string) (any, error) {
				return strconv.ParseInt(s, 10, 64)
			})
	case "uint":
		return convertNumber(entry.Value,
			func(n json.Number) (any, error) {
				v, err := strconv.ParseUint(n.String(), 10, 64)
				return uint(v), err
			},
			func(s string) (any, error) {
				v, err := strconv.ParseUint(s, 10, 64)
				return uint(v), err
			})
	case "uint8":
		return convertNumber(entry.Value,
			func(n json.Number) (any, error) {
				v, err := strconv.ParseUint(n.String(), 10, 8)
				return uint8(v), err
			},
			func(s string) (any, error) {
				v, err := strconv.ParseUint(s, 10, 8)
				return uint8(v), err
			})
	case "uint16":
		return convertNumber(entry.Value,
			func(n json.Number) (any, error) {
				v, err := strconv.ParseUint(n.String(), 10, 16)
				return uint16(v), err
			},
			func(s string) (any, error) {
				v, err := strconv.ParseUint(s, 10, 16)
				return uint16(v), err
			})
	case "uint32":
		return convertNumber(entry.Value,
			func(n json.Number) (any, error) {
				v, err := strconv.ParseUint(n.String(), 10, 32)
				return uint32(v), err
			},
			func(s string) (any, error) {
				v, err := strconv.ParseUint(s, 10, 32)
				return uint32(v), err
			})
	case "uint64":
		return convertNumber(entry.Value,
			func(n json.Number) (any, error) {
				return strconv.ParseUint(n.String(), 10, 64)
			},
			func(s string) (any, error) {
				return strconv.ParseUint(s, 10, 64)
			})
	case "float32":
		return convertNumber(entry.Value,
			func(n json.Number) (any, error) {
				v, err := n.Float64()
				return float32(v), err
			},
			func(s string) (any, error) {
				v, err := strconv.ParseFloat(s, 32)
				return float32(v), err
			})
	case "float64":
		return convertNumber(entry.Value,
			func(n json.Number) (any, error) {
				return n.Float64()
			},
			func(s string) (any, error) {
				return strconv.ParseFloat(s, 64)
			})
	case "string":
		if s, ok := entry.Value.(string); ok {
			return s, true
		}

	case "bytes": // 新增byte类型解析
		if str, ok := entry.Value.(string); ok {
			// 执行base64解码
			decoded, err := base64.StdEncoding.DecodeString(str)
			if err != nil {
				log.Printf("base64解码失败: %v", err)
				return nil, false
			}
			return decoded, true

		}
	case "list":
		// 将entry.Value转换为JSON数据，再反序列化为List
		data, err := json.Marshal(entry.Value)
		if err != nil {
			log.Printf("List数据转换失败: %v", err)
			return nil, false
		}
		var list List
		if err := json.Unmarshal(data, &list); err != nil {
			log.Printf("List反序列化失败: %v", err)
			return nil, false
		}
		return &list, true
	case "set":
		var s Set
		if err := json.Unmarshal([]byte(entry.Value.(string)), &s); err != nil {
			log.Printf("Set反序列化失败: %v", err)
			return nil, false
		}
		return &s, true
	default:
		return entry.Value, true
	}

	log.Printf("failed to convert value for type %s", entry.Type)
	return nil, false
}

func convertNumber(
	value interface{},
	numberConv func(json.Number) (any, error),
	strConv func(string) (any, error),
) (any, bool) {
	switch v := value.(type) {
	case json.Number:
		result, err := numberConv(v)
		if err != nil {
			log.Println(err)
			return nil, false
		}
		return result, true
	case string:
		if strConv != nil {
			result, err := strConv(v)
			if err != nil {
				log.Println(err)
				return nil, false
			}
			return result, true
		}
	}
	return nil, false
}

func (m *raftMap) SetIfExist(key string, value any) int {

	added := 1
	jsonData, err := json.Marshal(value)
	if err != nil {
		return 0
	}

	err = m.client.Put([]byte(key), jsonData)
	if err != nil {
		return 0

	}
	return added
}

func (m *raftMap) SetIfNotExist(key string, value any) int {
	added := 1
	jsonData, err := json.Marshal(value)
	if err != nil {
		return 0
	}

	err = m.client.Put([]byte(key), jsonData)
	if err != nil {
		return 0

	}
	return added
}

func (m *raftMap) Delete(key string) int {
	err := m.client.Delete([]byte(key))
	if err != nil {
		return 0
	} else {
		return 1
	}
}

// Keys return all stored keys in the concurrent map
func (m *raftMap) Keys() []string {
	s := make([]byte, 0)
	keys, _, err := m.client.Scan(s, 100)
	if err != nil {
		return nil
	}
	res := make([]string, 0)
	for _, key := range keys {
		res = append(res, string(key))
	}
	return res
}
