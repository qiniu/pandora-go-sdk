package pipeline

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetTrimedDataSchemaBase(t *testing.T) {
	var data map[string]interface{}
	dc := json.NewDecoder(strings.NewReader(`{"a":123,"b":123.1,"c":"123","d":true,"e":[1,2,3],"f":[1.2,2.1,3.1],"g":{"g1":"1"},"h":[],"i":[1,1],"j":[null,1,2]}`))
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
	imp := formValueType("i", PandoraTypeArray)
	imp.ElemType = PandoraTypeLong

	// 测试当数组的第一个元素为 null 时, 将该数组的元素类型设定为 string
	jmp := formValueType("j", PandoraTypeArray)
	jmp.ElemType = PandoraTypeString

	exp := map[string]RepoSchemaEntry{
		"a": formValueType("a", PandoraTypeLong),
		"b": formValueType("b", PandoraTypeFloat),
		"c": formValueType("c", PandoraTypeString),
		"d": formValueType("d", PandoraTypeBool),
		"e": emp,
		"f": fmp,
		"g": gmp,
		"i": imp,
		"j": jmp,
	}
	assert.NoError(t, err)
	vt := getTrimedDataSchema(data)
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
	vt = getTrimedDataSchema(data)
	assert.EqualValues(t, exp, vt)
}

func TestGetTrimedDataSchemaJsonString(t *testing.T) {
	jsontest := `{
   "client_uuid":"hongyaa_test_local",
   "created_at":"2017-07-24 15:55:13",
   "done_at":"2017-07-24 15:55:16",
   "expire":"14400",
   "id":"2",
   "imagename":"cirros",
   "job_id":"vNjOJlRgcQiCM5EuBZNF4AyMNzutcEeP",
   "lesson_id":"0",
   "msg":"",
   "process":"100",
   "resource":{
      "instance":[
         {
            "id":"daf6357b-471a-4b34-b571-569269787722",
            "name":"cirros_clone_504014877",
            "network":[
               {
                  "name":"match_public",
                  "id":"221f7dcb-dadd-4650-b4bf-5c111852a03b"
               }
            ],
            "adminPass":"hJo2VizqY7fD"
         }
      ]
   },
   "scene_id":"0",
   "status":"4",
   "type":"image",
   "updated_at":"2017-07-24 15:56:37",
   "user_id":"1"
}`
	var jsonobj map[string]interface{}
	err := json.Unmarshal([]byte(jsontest), &jsonobj)
	assert.NoError(t, err)
	gotschemas := getTrimedDataSchema(Data(jsonobj))

	var schemas []RepoSchemaEntry
	var keys sort.StringSlice
	for k := range gotschemas {
		keys = append(keys, k)
	}
	keys.Sort()
	for _, v := range keys {
		schemas = append(schemas, gotschemas[v])
	}

	expdsl := `client_uuid string
created_at string
done_at string
expire string
id string
imagename string
job_id string
lesson_id string
msg string
process string
resource map{
  instance array(string)
}
scene_id string
status string
type string
updated_at string
user_id string
`
	gotdsl := getFormatDSL(schemas, 0, "  ")
	assert.Equal(t, expdsl, gotdsl)
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

func TestCopyAndConvertData(t *testing.T) {
	data := Data{
		"a": "a",
		"b": 1,
		"c": nil,
		"e": []interface{}{},
		"d": map[string]interface{}{
			"a-1": "a",
			"b1":  1,
			"c-1": nil,
			"e1":  []interface{}{1, 2, "3"},
			"d1": map[string]interface{}{
				"a2":  "a",
				"b-2": 1,
				"c_2": nil,
				"e2":  []string{"a", "b", "c"},
				"d-2": map[string]interface{}{
					"a_3": "a",
					"b-3": 1,
					"c3":  nil,
					"e3":  []int{1, 2, 3, 4, 5},
					"d-3": map[string]interface{}{
						"a4": "a",
						"b4": 1,
						"c4": nil,
						"d4": map[string]interface{}{},
					},
					"f3": map[string]interface{}{
						"a4": "a",
						"b4": 1,
						"c4": nil,
						"d4": map[string]interface{}{
							"a5": "a",
							"b5": 1,
							"c5": nil,
							"d5": []string{"1", "2", "3", "4"},
						},
					},
					"g3": map[string]interface{}{
						"a4": "a",
						"b4": 1,
						"c4": nil,
						"d4": map[string]interface{}{
							"a5": "a",
							"b5": 1,
							"c5": nil,
							"d5": map[string]interface{}{
								"a6": "a",
								"b6": 1,
								"c6": nil,
								"d6": []int{1, 2, 3, 4, 5},
							},
						},
					},
				},
			},
		},
	}
	expData := Data{
		"a": "a",
		"b": 1,
		"d": map[string]interface{}{
			"a_1": "a",
			"b1":  1,
			"e1":  []interface{}{1, 2, "3"},
			"d1": map[string]interface{}{
				"a2":  "a",
				"b_2": 1,
				"e2":  []string{"a", "b", "c"},
				"d_2": map[string]interface{}{
					"a_3": "a",
					"b_3": 1,
					"e3":  []int{1, 2, 3, 4, 5},
					"d_3": map[string]interface{}{
						"a4": "a",
						"b4": 1,
						"d4": `{}`,
					},
					"f3": map[string]interface{}{
						"a4": "a",
						"b4": 1,
						"d4": `{"a5":"a","b5":1,"c5":null,"d5":["1","2","3","4"]}`,
					},
					"g3": map[string]interface{}{
						"a4": "a",
						"b4": 1,
						"d4": `{"a5":"a","b5":1,"c5":null,"d5":{"a6":"a","b6":1,"c6":null,"d6":[1,2,3,4,5]}}`,
					},
				},
			},
		},
	}
	gotData := copyAndConvertData(data, 1)
	if !reflect.DeepEqual(expData, gotData) {
		t.Fatalf("test error exp %v, but got %v", expData, gotData)
	}
}

func TestGetTrimedDataSchemaTrimNil(t *testing.T) {
	data := Data{
		"a": "a",
		"b": 1,
		"c": nil,
		"d": map[string]interface{}{
			"a-1": "a",
			"b1":  1,
			"c-1": []interface{}{},
			"d1": map[string]interface{}{
				"a2":  "a",
				"b-2": 1,
				"c_2": nil,
				"d-2": map[string]interface{}{
					"a_3": "a",
					"b-3": 1,
					"c3":  []interface{}{nil, 1, 2, 4},
					"d-3": []string{"a", "b", "c"},
				},
			},
		},
	}
	expData := Data{
		"a": "a",
		"b": 1,
		"d": map[string]interface{}{
			"a-1": "a",
			"b1":  1,
			"d1": map[string]interface{}{
				"a2":  "a",
				"b-2": 1,
				"d-2": map[string]interface{}{
					"a_3": "a",
					"b-3": 1,
					"c3":  []interface{}{nil, 1, 2, 4},
					"d-3": []string{"a", "b", "c"},
				},
			},
		},
	}
	getTrimedDataSchema(data)
	if !reflect.DeepEqual(expData, data) {
		t.Fatalf("test error exp %v, but got %v", expData, data)
	}
}
