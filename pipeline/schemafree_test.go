package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetPandoraKeyValueType(t *testing.T) {
	var data map[string]interface{}
	dc := json.NewDecoder(strings.NewReader("{\"a\":123,\"b\":123.1,\"c\":\"123\",\"d\":true,\"e\":[1,2,3],\"f\":[1.2,2.1,3.1],\"g\":{\"g1\":\"1\"}}"))
	dc.UseNumber()
	err := dc.Decode(&data)
	emp := formValueType("e", PandoraTypeArray)
	emp.ElemType = PandoraTypeLong
	fmp := formValueType("f", PandoraTypeArray)
	fmp.ElemType = PandoraTypeFloat
	gmp := formValueType("g", PandoraTypeMap)
	gmp.Schema = []RepoSchemaEntry{
		RepoSchemaEntry{
			Key:       "g1",
			ValueType: PandoraTypeString,
		},
	}

	exp := map[string]RepoSchemaEntry{
		"a": formValueType("a", PandoraTypeLong),
		"b": formValueType("b", PandoraTypeFloat),
		"c": formValueType("c", PandoraTypeString),
		"d": formValueType("d", PandoraTypeBool),
		"e": emp,
		"f": fmp,
		"g": gmp,
	}
	assert.NoError(t, err)
	vt := getPandoraKeyValueType(data)
	assert.Equal(t, exp, vt)
	data = map[string]interface{}{
		"a": 1,
		"b": time.Now().Format(time.RFC3339Nano),
		"c": time.Now().Format(time.RFC3339),
		"d": 1.0,
		"e": int64(32),
		"f": "123",
		"g": true,
		"m": nil,
		"h": map[string]interface{}{
			"h5": map[string]interface{}{
				"h51": 1,
			},
		},
		"h1": map[string]interface{}{
			"h1": 123,
		},
		"h2": map[string]interface{}{
			"h2": "123",
		},
		"h3": map[string]interface{}{
			"h3": 123.1,
		},
		"h4": map[string]interface{}{
			"h4": map[string]interface{}{},
		},
		"i": false,
	}
	hmp := formValueType("h", PandoraTypeMap)
	hmp.Schema = []RepoSchemaEntry{
		RepoSchemaEntry{
			Key:       "h5",
			ValueType: PandoraTypeMap,
			Schema: []RepoSchemaEntry{
				RepoSchemaEntry{
					Key:       "h51",
					ValueType: PandoraTypeLong,
				},
			},
		},
	}
	hmp1 := formValueType("h1", PandoraTypeMap)
	hmp1.Schema = []RepoSchemaEntry{
		RepoSchemaEntry{
			Key:       "h1",
			ValueType: PandoraTypeLong,
		},
	}
	hmp2 := formValueType("h2", PandoraTypeMap)
	hmp2.Schema = []RepoSchemaEntry{
		RepoSchemaEntry{
			Key:       "h2",
			ValueType: PandoraTypeString,
		},
	}
	hmp3 := formValueType("h3", PandoraTypeMap)
	hmp3.Schema = []RepoSchemaEntry{
		RepoSchemaEntry{
			Key:       "h3",
			ValueType: PandoraTypeFloat,
		},
	}
	hmp4 := formValueType("h4", PandoraTypeMap)
	hmp4.Schema = []RepoSchemaEntry{
		RepoSchemaEntry{
			Key:       "h4",
			ValueType: PandoraTypeMap,
		},
	}

	exp = map[string]RepoSchemaEntry{
		"a":  formValueType("a", PandoraTypeLong),
		"b":  formValueType("b", PandoraTypeDate),
		"c":  formValueType("c", PandoraTypeDate),
		"d":  formValueType("d", PandoraTypeFloat),
		"e":  formValueType("e", PandoraTypeLong),
		"f":  formValueType("f", PandoraTypeString),
		"g":  formValueType("g", PandoraTypeBool),
		"h":  hmp,
		"h1": hmp1,
		"h2": hmp2,
		"h3": hmp3,
		"h4": hmp4,

		"i": formValueType("i", PandoraTypeBool),
	}
	vt = getPandoraKeyValueType(data)
	assert.EqualValues(t, exp, vt)
}

func TestDeepDeleteCheck(t *testing.T) {
	tests := []struct {
		value  interface{}
		left   interface{}
		schema RepoSchemaEntry
		exp    bool
	}{
		{
			value: map[string]interface{}{},
			left:  map[string]interface{}{},
			schema: RepoSchemaEntry{
				Key:       "hello",
				ValueType: PandoraTypeMap,
			},
			exp: true,
		},
		{
			value: 123,
			left:  123,
			schema: RepoSchemaEntry{
				Key:       "hello",
				ValueType: PandoraTypeMap,
			},
			exp: true,
		},
		{
			value: map[string]interface{}{},
			left:  map[string]interface{}{},
			schema: RepoSchemaEntry{
				Key:       "hello",
				ValueType: PandoraTypeLong,
			},
			exp: true,
		},
		{
			value: map[string]interface{}{
				"x": 123,
			},
			left: map[string]interface{}{
				"x": 123,
			},
			schema: RepoSchemaEntry{
				Key:       "hello",
				ValueType: PandoraTypeMap,
				Schema:    []RepoSchemaEntry{},
			},
			exp: false,
		},
		{
			value: map[string]interface{}{
				"x": 123,
			},
			left: map[string]interface{}{
				"x": 123,
			},
			schema: RepoSchemaEntry{
				Key:       "hello",
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					RepoSchemaEntry{
						Key:       "x",
						ValueType: PandoraTypeLong,
					},
				},
			},
			exp: true,
		},
		{
			value: map[string]interface{}{
				"x": 123,
			},
			left: map[string]interface{}{
				"x": 123,
			},
			schema: RepoSchemaEntry{
				Key:       "hello",
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					RepoSchemaEntry{
						Key:       "x",
						ValueType: PandoraTypeMap,
					},
				},
			},
			exp: true,
		},
		{
			value: map[string]interface{}{
				"x": map[string]interface{}{},
			},
			left: map[string]interface{}{
				"x": map[string]interface{}{},
			},
			schema: RepoSchemaEntry{
				Key:       "hello",
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					RepoSchemaEntry{
						Key:       "x",
						ValueType: PandoraTypeLong,
					},
				},
			},
			exp: true,
		},
		{
			value: map[string]interface{}{
				"x": map[string]interface{}{
					"y": 123,
				},
			},
			left: map[string]interface{}{
				"x": map[string]interface{}{
					"y": 123,
				},
			},
			schema: RepoSchemaEntry{
				Key:       "hello",
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					RepoSchemaEntry{
						Key:       "x",
						ValueType: PandoraTypeMap,
					},
				},
			},
			exp: false,
		},
		{
			value: map[string]interface{}{
				"x": map[string]interface{}{
					"y": 123,
				},
			},
			left: map[string]interface{}{
				"x": map[string]interface{}{
					"y": 123,
				},
			},
			schema: RepoSchemaEntry{
				Key:       "hello",
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					RepoSchemaEntry{
						Key:       "x",
						ValueType: PandoraTypeMap,
						Schema: []RepoSchemaEntry{
							RepoSchemaEntry{
								Key: "y",
							},
						},
					},
				},
			},
			exp: true,
		},
		{
			value: map[string]interface{}{
				"x": map[string]interface{}{
					"y": 123,
				},
				"z": 123,
			},
			left: map[string]interface{}{
				"x": map[string]interface{}{
					"y": 123,
				},
				"z": 123,
			},
			schema: RepoSchemaEntry{
				Key:       "hello",
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					RepoSchemaEntry{
						Key:       "x",
						ValueType: PandoraTypeMap,
						Schema: []RepoSchemaEntry{
							RepoSchemaEntry{
								Key: "y",
							},
						},
					},
				},
			},
			exp: false,
		},
		{
			value: map[string]interface{}{
				"x": map[string]interface{}{
					"y": 123,
					"z": 123,
				},
			},
			left: map[string]interface{}{
				"x": map[string]interface{}{
					"y": 123,
					"z": 123,
				},
			},
			schema: RepoSchemaEntry{
				Key:       "hello",
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					RepoSchemaEntry{
						Key:       "x",
						ValueType: PandoraTypeMap,
						Schema: []RepoSchemaEntry{
							RepoSchemaEntry{
								Key: "y",
							},
							RepoSchemaEntry{
								Key: "z",
							},
						},
					},
				},
			},
			exp: true,
		},
		{
			value: map[string]interface{}{
				"x": map[string]interface{}{
					"y": 123,
					"z": 123,
					"a": true,
				},
			},
			left: map[string]interface{}{
				"x": map[string]interface{}{
					"y": 123,
					"z": 123,
					"a": true,
				},
			},
			schema: RepoSchemaEntry{
				Key:       "hello",
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					RepoSchemaEntry{
						Key:       "x",
						ValueType: PandoraTypeMap,
						Schema: []RepoSchemaEntry{
							RepoSchemaEntry{
								Key: "y",
							},
							RepoSchemaEntry{
								Key: "z",
							},
						},
					},
				},
			},
			exp: false,
		},
	}
	for _, ti := range tests {
		got := deepDeleteCheck(ti.value, ti.schema)
		assert.Equal(t, ti.exp, got)
		assert.Equal(t, ti.left, ti.value)
	}
}

func TestMergePandoraSchemas(t *testing.T) {
	tests := []struct {
		oldScs []RepoSchemaEntry
		newScs []RepoSchemaEntry
		exp    []RepoSchemaEntry
		err    bool
	}{
		{
			oldScs: []RepoSchemaEntry{},
			newScs: []RepoSchemaEntry{},
			exp:    []RepoSchemaEntry{},
		},
		{
			oldScs: []RepoSchemaEntry{},
			newScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "abc"},
			},
			exp: []RepoSchemaEntry{RepoSchemaEntry{Key: "abc"}},
		},
		{
			oldScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "abc"},
			},
			newScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "abc"},
			},
			exp: []RepoSchemaEntry{RepoSchemaEntry{Key: "abc"}},
		},
		{
			oldScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "abc", ValueType: "string"},
			},
			newScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "abc", ValueType: "float"},
			},
			exp: []RepoSchemaEntry{RepoSchemaEntry{Key: "abc"}},
			err: true,
		},
		{
			oldScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "a"},
			},
			newScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "b"},
			},
			exp: []RepoSchemaEntry{RepoSchemaEntry{Key: "a"}, RepoSchemaEntry{Key: "b"}},
		},
		{
			oldScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "a"},
			},
			newScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "a"},
				RepoSchemaEntry{Key: "b"},
			},
			exp: []RepoSchemaEntry{RepoSchemaEntry{Key: "a"}, RepoSchemaEntry{Key: "b"}},
		},
		{
			oldScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "b"},
				RepoSchemaEntry{Key: "c"},
			},
			newScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "a"},
				RepoSchemaEntry{Key: "b"},
			},
			exp: []RepoSchemaEntry{RepoSchemaEntry{Key: "a"}, RepoSchemaEntry{Key: "b"}, RepoSchemaEntry{Key: "c"}},
		},
		{
			oldScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "a"},
				}},
				RepoSchemaEntry{Key: "c"},
			},
			newScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "a"},
				RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "b"},
				}},
			},
			exp: []RepoSchemaEntry{RepoSchemaEntry{Key: "a"}, RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "a"},
				RepoSchemaEntry{Key: "b"},
			}}, RepoSchemaEntry{Key: "c"}},
		},
		{
			oldScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "a"},
				}},
				RepoSchemaEntry{Key: "c"},
			},
			newScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "a"},
				RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "a"},
				}},
			},
			exp: []RepoSchemaEntry{RepoSchemaEntry{Key: "a"}, RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "a"},
			}}, RepoSchemaEntry{Key: "c"}},
		},
		{
			oldScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "y"},
				}},
				RepoSchemaEntry{Key: "c"},
			},
			newScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "a"},
				RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "x"},
				}},
			},
			exp: []RepoSchemaEntry{RepoSchemaEntry{Key: "a"}, RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "x"},
				RepoSchemaEntry{Key: "y"},
			}}, RepoSchemaEntry{Key: "c"}},
		},
		{
			oldScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "y", ValueType: PandoraTypeMap},
				}},
				RepoSchemaEntry{Key: "c"},
			},
			newScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "a"},
				RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "y", ValueType: PandoraTypeString},
				}},
			},
			exp: []RepoSchemaEntry{RepoSchemaEntry{Key: "a"}, RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "x"},
				RepoSchemaEntry{Key: "y"},
			}}, RepoSchemaEntry{Key: "c"}},
			err: true,
		},
		{
			oldScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "y", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
						RepoSchemaEntry{Key: "11"},
					}},
				}},
				RepoSchemaEntry{Key: "c"},
			},
			newScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "a"},
				RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "y", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
						RepoSchemaEntry{Key: "11"},
					}},
				}},
			},
			exp: []RepoSchemaEntry{RepoSchemaEntry{Key: "a"}, RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "y", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "11"},
				}},
			}}, RepoSchemaEntry{Key: "c"}},
		},
		{
			oldScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "y", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
						RepoSchemaEntry{Key: "11"},
					}},
				}},
				RepoSchemaEntry{Key: "c"},
			},
			newScs: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "a"},
				RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "y", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
						RepoSchemaEntry{Key: "21"},
						RepoSchemaEntry{Key: "11"},
					}},
				}},
			},
			exp: []RepoSchemaEntry{RepoSchemaEntry{Key: "a"}, RepoSchemaEntry{Key: "b", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
				RepoSchemaEntry{Key: "y", ValueType: PandoraTypeMap, Schema: []RepoSchemaEntry{
					RepoSchemaEntry{Key: "11"},
					RepoSchemaEntry{Key: "21"},
				}},
			}}, RepoSchemaEntry{Key: "c"}},
		},
	}
	for idx, ti := range tests {
		got, err := mergePandoraSchemas(ti.oldScs, ti.newScs)
		if ti.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, ti.exp, got, fmt.Sprintf("index %v", idx))
		}
	}
}

func TestCheckIgnore(t *testing.T) {
	tests := []struct {
		v   interface{}
		tp  string
		exp bool
	}{
		{
			exp: true,
		},
		{
			v:   "",
			tp:  PandoraTypeJsonString,
			exp: true,
		},
		{
			v:   "",
			tp:  PandoraTypeString,
			exp: false,
		},
		{
			v:   nil,
			tp:  PandoraTypeJsonString,
			exp: true,
		},
		{
			v:   "xs",
			tp:  PandoraTypeJsonString,
			exp: false,
		},
		{
			v:   "xs",
			tp:  PandoraTypeString,
			exp: false,
		},
		{
			v:   123,
			tp:  PandoraTypeFloat,
			exp: false,
		},
	}
	for _, ti := range tests {
		got := checkIgnore(ti.v, ti.tp)
		assert.Equal(t, ti.exp, got)
	}
}

func TestConvertData(t *testing.T) {
	type helloint int
	tests := []struct {
		v      interface{}
		schema RepoSchemaEntry
		exp    interface{}
	}{
		{
			v: helloint(1),
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeLong,
			},
			exp: helloint(1),
		},
		{
			v: helloint(1),
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeString,
			},
			exp: "1",
		},
		{
			v: json.Number("1"),
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeLong,
			},
			exp: int64(1),
		},
		{
			v: "1",
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeLong,
			},
			exp: int64(1),
		},
		{
			v: []int{1, 2, 3},
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeArray,
				ElemType:  PandoraTypeLong,
			},
			exp: []interface{}{1, 2, 3},
		},
		{
			v: []int{1, 2, 3},
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeArray,
				ElemType:  PandoraTypeString,
			},
			exp: []interface{}{"1", "2", "3"},
		},
		{
			v: []interface{}{1, 2, 3},
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeArray,
				ElemType:  PandoraTypeString,
			},
			exp: []interface{}{"1", "2", "3"},
		},
		{
			v: `[1, 2, 3]`,
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeArray,
				ElemType:  PandoraTypeString,
			},
			exp: []interface{}{"1", "2", "3"},
		},
		{
			v: `["1", "2", "3"]`,
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeArray,
				ElemType:  PandoraTypeFloat,
			},
			exp: []interface{}{float64(1), float64(2), float64(3)},
		},
		{
			v: "1.1",
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeFloat,
			},
			exp: float64(1.1),
		},
		{
			v: map[string]interface{}{
				"a": 123,
			},
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					{ValueType: PandoraTypeString, Key: "a"},
				},
			},
			exp: map[string]interface{}{
				"a": "123",
			},
		},
		{
			v: map[string]interface{}{
				"a": 123,
			},
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					{ValueType: PandoraTypeFloat, Key: "a"},
				},
			},
			exp: map[string]interface{}{
				"a": 123,
			},
		},
		{
			v: map[string]interface{}{
				"a": "123",
				"b": "hello",
			},
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					{ValueType: PandoraTypeFloat, Key: "a"},
					{ValueType: PandoraTypeString, Key: "b"},
				},
			},
			exp: map[string]interface{}{
				"a": float64(123),
				"b": "hello",
			},
		},
		{
			v: `{
				"a": "123",
				"b": "hello"
			}`,
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					{ValueType: PandoraTypeFloat, Key: "a"},
					{ValueType: PandoraTypeString, Key: "b"},
				},
			},
			exp: map[string]interface{}{
				"a": float64(123),
				"b": "hello",
			},
		},
		{
			v: `{
				"a": "123.23",
				"b": "hello"
			}`,
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					{ValueType: PandoraTypeLong, Key: "a"},
					{ValueType: PandoraTypeString, Key: "b"},
				},
			},
			exp: map[string]interface{}{
				"a": int64(123),
				"b": "hello",
			},
		},
		{
			v: `{
				"a": "123.23",
				"b": {
					"c":123
				}
			}`,
			schema: RepoSchemaEntry{
				ValueType: PandoraTypeMap,
				Schema: []RepoSchemaEntry{
					{ValueType: PandoraTypeLong, Key: "a"},
					{ValueType: PandoraTypeMap, Key: "b", Schema: []RepoSchemaEntry{
						{ValueType: PandoraTypeLong, Key: "c"}},
					},
				},
			},
			exp: map[string]interface{}{
				"a": int64(123),
				"b": map[string]interface{}{
					"c": int64(123),
				},
			},
		},
	}
	for _, ti := range tests {
		got, err := dataConvert(ti.v, ti.schema)
		assert.NoError(t, err)
		assert.Equal(t, ti.exp, got)
	}
}
