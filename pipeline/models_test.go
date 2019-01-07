package pipeline

import (
	"testing"

	"time"

	"github.com/stretchr/testify/assert"
)

func Test_convertDSL(t *testing.T) {
	tests := []struct {
		dsl    string
		exp    []RepoSchemaEntry
		experr bool
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
			dsl: `x1 l, x2 *f, x3 *s, x9 d,x4 a(s),x5 *m{x7 string},x6 b,
			x8,x10 m{x11 d,x12 a(f),
			x13 m{x14},x15 m{x16 l}}`,
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
		{
			dsl: "baoge l,baoge,",
			exp: []RepoSchemaEntry{
				RepoSchemaEntry{
					Key:       "baoge",
					ValueType: "long",
				},
				RepoSchemaEntry{
					Key:       "baoge",
					ValueType: "string",
				},
			},
			experr: true,
		},
		{
			dsl: "invalid-key l, invalid-key2 l, validKey,x-10 {x-11 d,x12 (f),x13 {x-14},x-15 {x16 l}}",
			exp: []RepoSchemaEntry{
				{
					Key:       "invalid_key",
					ValueType: "long",
				},
				{
					Key:       "invalid_key2",
					ValueType: "long",
				},
				{
					Key:       "validKey",
					ValueType: "string",
				},
				{
					Key:       "x_10",
					ValueType: "map",
					Schema: []RepoSchemaEntry{
						{
							Key:       "x_11",
							ValueType: "date",
						},
						{
							Key:       "x12",
							ValueType: "array",
							ElemType:  "float",
						},
						{
							Key:       "x13",
							ValueType: "map",
							Schema: []RepoSchemaEntry{
								{
									Key:       "x_14",
									ValueType: "string",
								},
							},
						},
						{
							Key:       "x_15",
							ValueType: "map",
							Schema: []RepoSchemaEntry{
								{
									Key:       "x16",
									ValueType: "long",
								},
							},
						},
					},
				},
			},
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
		dslstr := getFormatDSL(ti.exp, 0, "\t")
		got2, err := toSchema(dslstr, 0)
		assert.NoError(t, err)
		assert.Equal(t, ti.exp, got2)
	}
}

func Test_PandoraKey(t *testing.T) {
	testKeys := []string{"", "@timestamp", ".dot", "percent%100", "^^^^^^^^^^", "timestamp", "__disk", "___disk///a/b/__c__", "_disk__//@_a"}
	expectKeys := []string{"KEmptyPandoraAutoAdd", "timestamp", "dot", "percent_100", "", "timestamp", "disk", "disk_a_b___c__", "disk____a"}
	expectValid := []bool{false, false, false, false, false, true, false, false, false}
	for idx, key := range testKeys {
		actual, valid := PandoraKey(key)
		assert.Equal(t, expectKeys[idx], actual)
		assert.Equal(t, expectValid[idx], valid)
	}
}

func BenchmarkPandoraKey(b *testing.B) {
	b.ReportAllocs()
	testKeys := []string{"@timestamp", ".dot", "percent%100", "^^^^^^^^^^", "timestamp", "aaa"}
	for i := 0; i < b.N; i++ {
		for _, key := range testKeys {
			PandoraKey(key)
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

func TestEscapeString(t *testing.T) {
	got := escapeStringField("\t\n")
	assert.Equal(t, "\\t\\n", got)
	got = escapeStringField("a\tb\nc")
	assert.Equal(t, "a\\tb\\nc", got)
	got = escapeStringField("\\t\\n")
	assert.Equal(t, "\\t\\n", got)
}

func TestEscapeBytesField(t *testing.T) {
	got := escapeBytesField([]byte("\t\n"))
	assert.Equal(t, []byte("\\t\\n"), got)
	got = escapeBytesField([]byte("a\tb\nc"))
	assert.Equal(t, []byte("a\\tb\\nc"), got)
	got = escapeBytesField([]byte("\\t\\n"))
	assert.Equal(t, []byte("\\t\\n"), got)
}

var BencS string

/*
20000000	        59.6 ns/op
PASS
*/
func BenchmarkEscapeString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		BencS = escapeStringField("\t\n")
	}
	_ = BencS
}

var BencB []byte

/*
30000000	        45.1 ns/op
PASS
*/
func BenchmarkEscapeBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		BencB = escapeBytesField([]byte("\t\n"))
	}
	_ = BencB
}

/*
10000000	       148 ns/op
PASS
*/
func BenchmarkPointFieldString(b *testing.B) {
	p := &PointField{
		"hello",
		"nihao!",
	}
	for i := 0; i < b.N; i++ {
		BencS = p.String()
	}
	_ = BencS
}

func BenchmarkPointFieldBytes(b *testing.B) {
	p := &PointField{
		"hello",
		"nihao!",
	}
	for i := 0; i < b.N; i++ {
		BencB = p.Bytes()
	}
	_ = BencB
}

func TestPointField_Bytes(t *testing.T) {
	p := &PointField{
		"hello",
		"nihao!",
	}
	assert.Equal(t, p.String(), string(p.Bytes()))

	p = &PointField{
		"hello",
		12,
	}
	assert.Equal(t, p.String(), string(p.Bytes()))

	p = &PointField{
		"hello",
		12.3,
	}
	assert.Equal(t, p.String(), string(p.Bytes()))

	p = &PointField{
		"hello",
		"nihao\ta\n",
	}
	assert.Equal(t, p.String(), string(p.Bytes()))

	p = &PointField{
		"hello",
		map[string]interface{}{"a": "B", "c": 1},
	}
	assert.Equal(t, p.String(), string(p.Bytes()))

	p = &PointField{
		"hello",
		[]map[string]interface{}{{"a": "B", "c": 1}, {"heha": 12.1}},
	}
	assert.Equal(t, p.String(), string(p.Bytes()))
}

func BenchmarkPointString(b *testing.B) {
	p := PointField{
		"hello",
		"nihao!",
	}
	newp := &Point{Fields: []PointField{p, {"ha", ""}}}
	for i := 0; i < b.N; i++ {
		BencS = newp.ToString()
	}
	_ = BencS
}

func BenchmarkPointBytes(b *testing.B) {
	p := PointField{
		"hello",
		"nihao!",
	}
	newp := &Point{Fields: []PointField{p, {"ha", ""}}}
	for i := 0; i < b.N; i++ {
		BencB = newp.ToBytes()
	}
	_ = BencB
}

func TestDateTime(t *testing.T) {
	tm := time.Unix(10, 0)
	p := PointField{
		Key:   "t",
		Value: time.Unix(10, 0),
	}
	assert.Equal(t, string(p.Bytes()), p.String())
	p = PointField{
		Key:   "t",
		Value: &tm,
	}
	var np *time.Time
	assert.Equal(t, string(p.Bytes()), p.String())
	p = PointField{
		Key:   "t",
		Value: np,
	}
	assert.Equal(t, string(p.Bytes()), p.String())
}
