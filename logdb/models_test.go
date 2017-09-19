package logdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_convertDSL(t *testing.T) {
	tests := []struct {
		dsl    string
		exp    []RepoSchemaEntry
		experr bool
	}{
		{
			dsl: "x1 l,x2, ,x3 s keyword,x4 float,x5 long,x6 map{x7 boolean,x8 array(l)},,",
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
					Analyzer:  "keyword",
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
					ValueType: "object",
					Schemas: []RepoSchemaEntry{
						RepoSchemaEntry{
							Key:       "x7",
							ValueType: "boolean",
						},
						RepoSchemaEntry{
							Key:       "x8",
							ValueType: "long",
						},
					},
				},
			},
		},
		{
			dsl: "x1 l, x2 f, x3 *s, x9 d,x4 a(s),x5 o{x7 string index_ansj},x6 b,x8,x10 m{x11 d,x12 a(f),x13 m{x14},x15 m{x16 l}}",
			exp: []RepoSchemaEntry{
				RepoSchemaEntry{
					Key:       "x1",
					ValueType: "long",
				},
				RepoSchemaEntry{
					Key:       "x2",
					ValueType: "float",
				},
				RepoSchemaEntry{
					Key:       "x3",
					ValueType: "string",
					Primary:   true,
				},
				RepoSchemaEntry{
					Key:       "x9",
					ValueType: "date",
				},
				RepoSchemaEntry{
					Key:       "x4",
					ValueType: "string",
				},
				RepoSchemaEntry{
					Key:       "x5",
					ValueType: "object",
					Schemas: []RepoSchemaEntry{RepoSchemaEntry{
						Key:       "x7",
						ValueType: "string",
						Analyzer:  "index_ansj",
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
					ValueType: "object",
					Schemas: []RepoSchemaEntry{
						RepoSchemaEntry{
							Key:       "x11",
							ValueType: "date",
						},
						RepoSchemaEntry{
							Key:       "x12",
							ValueType: "float",
						},
						RepoSchemaEntry{
							Key:       "x13",
							ValueType: "object",
							Schemas: []RepoSchemaEntry{RepoSchemaEntry{
								Key:       "x14",
								ValueType: "string",
							}},
						},
						RepoSchemaEntry{
							Key:       "x15",
							ValueType: "object",
							Schemas: []RepoSchemaEntry{{
								Key:       "x16",
								ValueType: "long",
							}},
						},
					},
				},
			},
		},
		{
			dsl: "x1 l, x2 f, x3 *s, x9 d,x4 (s),x5 {x7 string},x6 b,x8,x10 {x11 d,x12 (f),x13 {x14},x15 {x16 l}}",
			exp: []RepoSchemaEntry{
				RepoSchemaEntry{
					Key:       "x1",
					ValueType: "long",
				},
				RepoSchemaEntry{
					Key:       "x2",
					ValueType: "float",
				},
				RepoSchemaEntry{
					Key:       "x3",
					ValueType: "string",
					Primary:   true,
				},
				RepoSchemaEntry{
					Key:       "x9",
					ValueType: "date",
				},
				RepoSchemaEntry{
					Key:       "x4",
					ValueType: "string",
				},
				RepoSchemaEntry{
					Key:       "x5",
					ValueType: "object",
					Schemas: []RepoSchemaEntry{RepoSchemaEntry{
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
					ValueType: "object",
					Schemas: []RepoSchemaEntry{
						RepoSchemaEntry{
							Key:       "x11",
							ValueType: "date",
						},
						RepoSchemaEntry{
							Key:       "x12",
							ValueType: "float",
						},
						RepoSchemaEntry{
							Key:       "x13",
							ValueType: "object",
							Schemas: []RepoSchemaEntry{RepoSchemaEntry{
								Key:       "x14",
								ValueType: "string",
							}},
						},
						RepoSchemaEntry{
							Key:       "x15",
							ValueType: "object",
							Schemas: []RepoSchemaEntry{{
								Key:       "x16",
								ValueType: "long",
							}},
						},
					},
				},
			},
		},
		{
			dsl: "baoge l, baoge f",
			exp: []RepoSchemaEntry{
				RepoSchemaEntry{
					Key:       "baoge",
					ValueType: "long",
				},
				RepoSchemaEntry{
					Key:       "baoge",
					ValueType: "float",
				},
			},
			experr: true,
		},
	}
	for _, ti := range tests {
		got, err := toSchema(ti.dsl, 0)
		if ti.experr {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, ti.exp, got)

		newdsl := SchemaToDSL(ti.exp, "\t")
		got2, err := toSchema(newdsl, 0)
		assert.NoError(t, err)
		assert.Equal(t, ti.exp, got2)
	}
}

func TestCheckRetention(t *testing.T) {
	fullTextSearch := noRetentionRepo
	if err := checkRetention(fullTextSearch); err != nil {
		t.Error(err)
	}
}
