package pipeline

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_convertDSL(t *testing.T) {
	tests := []struct {
		dsl string
		exp []RepoSchemaEntry
	}{
		{
			dsl: "x1 l,x2, ,x3 s,x4 float,x5 long,x6 map{x7 *boolean,x8 array(l)},,",
			exp: []RepoSchemaEntry{
				RepoSchemaEntry{
					Key:       "x1",
					ValueType: "long",
				},
				RepoSchemaEntry{
					Key:       "x2",
					ValueType: "string",
				},
				RepoSchemaEntry{
					Key:       "x3",
					ValueType: "string",
				},
				RepoSchemaEntry{
					Key:       "x4",
					ValueType: "float",
				},
				RepoSchemaEntry{
					Key:       "x5",
					ValueType: "long",
				},
				RepoSchemaEntry{
					Key:       "x6",
					ValueType: "map",
					Schema: []RepoSchemaEntry{
						RepoSchemaEntry{
							Key:       "x7",
							ValueType: "boolean",
							Required:  true,
						},
						RepoSchemaEntry{
							Key:       "x8",
							ValueType: "array",
							ElemType:  "long",
						},
					},
				},
			},
		},
		{
			dsl: "x1 l, x2 *f, x3 *s, x9 d,x4 a(s),x5 *m{x7 string},x6 b,x8,x10 m{x11 d,x12 a(f),x13 m{x14},x15 m{x16 l}}",
			exp: []RepoSchemaEntry{
				RepoSchemaEntry{
					Key:       "x1",
					ValueType: "long",
				},
				RepoSchemaEntry{
					Key:       "x2",
					ValueType: "float",
					Required:  true,
				},
				RepoSchemaEntry{
					Key:       "x3",
					ValueType: "string",
					Required:  true,
				},
				RepoSchemaEntry{
					Key:       "x9",
					ValueType: "date",
				},
				RepoSchemaEntry{
					Key:       "x4",
					ValueType: "array",
					ElemType:  "string",
				},
				RepoSchemaEntry{
					Key:       "x5",
					ValueType: "map",
					Required:  true,
					Schema: []RepoSchemaEntry{RepoSchemaEntry{
						Key:       "x7",
						ValueType: "string",
					}},
				},
				RepoSchemaEntry{
					Key:       "x6",
					ValueType: "boolean",
				},
				RepoSchemaEntry{
					Key:       "x8",
					ValueType: "string",
				},
				RepoSchemaEntry{
					Key:       "x10",
					ValueType: "map",
					Schema: []RepoSchemaEntry{
						RepoSchemaEntry{
							Key:       "x11",
							ValueType: "date",
						},
						RepoSchemaEntry{
							Key:       "x12",
							ValueType: "array",
							ElemType:  "float",
						},
						RepoSchemaEntry{
							Key:       "x13",
							ValueType: "map",
							Schema: []RepoSchemaEntry{RepoSchemaEntry{
								Key:       "x14",
								ValueType: "string",
							}},
						},
						RepoSchemaEntry{
							Key:       "x15",
							ValueType: "map",
							Schema: []RepoSchemaEntry{{
								Key:       "x16",
								ValueType: "long",
							}},
						},
					},
				},
			},
		},
		{
			dsl: "x1 l, x2 *f, x3 *s, x9 d,x4 (s),x5 *{x7 string},x6 b,x8,x10 {x11 d,x12 (f),x13 {x14},x15 {x16 l}}",
			exp: []RepoSchemaEntry{
				RepoSchemaEntry{
					Key:       "x1",
					ValueType: "long",
				},
				RepoSchemaEntry{
					Key:       "x2",
					ValueType: "float",
					Required:  true,
				},
				RepoSchemaEntry{
					Key:       "x3",
					ValueType: "string",
					Required:  true,
				},
				RepoSchemaEntry{
					Key:       "x9",
					ValueType: "date",
				},
				RepoSchemaEntry{
					Key:       "x4",
					ValueType: "array",
					ElemType:  "string",
				},
				RepoSchemaEntry{
					Key:       "x5",
					ValueType: "map",
					Required:  true,
					Schema: []RepoSchemaEntry{RepoSchemaEntry{
						Key:       "x7",
						ValueType: "string",
					}},
				},
				RepoSchemaEntry{
					Key:       "x6",
					ValueType: "boolean",
				},
				RepoSchemaEntry{
					Key:       "x8",
					ValueType: "string",
				},
				RepoSchemaEntry{
					Key:       "x10",
					ValueType: "map",
					Schema: []RepoSchemaEntry{
						RepoSchemaEntry{
							Key:       "x11",
							ValueType: "date",
						},
						RepoSchemaEntry{
							Key:       "x12",
							ValueType: "array",
							ElemType:  "float",
						},
						RepoSchemaEntry{
							Key:       "x13",
							ValueType: "map",
							Schema: []RepoSchemaEntry{RepoSchemaEntry{
								Key:       "x14",
								ValueType: "string",
							}},
						},
						RepoSchemaEntry{
							Key:       "x15",
							ValueType: "map",
							Schema: []RepoSchemaEntry{{
								Key:       "x16",
								ValueType: "long",
							}},
						},
					},
				},
			},
		},
	}
	for _, ti := range tests {
		got, err := toSchema(ti.dsl, 0)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(ti.exp, got) {
			t.Error("should be equal")
		}
	}
}

func Test_PointField(t *testing.T) {
	p := &PointField{Key: "a", Value: "123"}
	assert.Equal(t, "a=123\t", p.String())
	p = &PointField{Key: "b", Value: ""}
	assert.Equal(t, "b=\t", p.String())
	type xs string
	p = &PointField{Key: "c", Value: xs("456")}
	assert.Equal(t, "c=456\t", p.String())
}

func BenchmarkPointField1(b *testing.B) {
	p := &PointField{Key: "a", Value: "123"}
	for i := 0; i < b.N; i++ {
		p.String()
	}
	/*
		10000000	       157 ns/op
		PASS
	*/
}

func BenchmarkPointField2(b *testing.B) {
	type xs string
	p := &PointField{Key: "c", Value: xs("456")}
	for i := 0; i < b.N; i++ {
		p.String()
	}
	/*
		10000000	       146 ns/op
		PASS
	*/
}
