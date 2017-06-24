package pipeline

import (
	"encoding/json"
	"reflect"
	"time"

	"sort"

	"fmt"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

func (c *Pipeline) getSchemas(repoName string) (schemas map[string]RepoSchemaEntry, err error) {
	repo, err := c.GetRepo(&GetRepoInput{
		RepoName: repoName,
	})
	if err != nil {
		return
	}
	schemas = make(map[string]RepoSchemaEntry)
	for _, v := range repo.Schema {
		schemas[v.Key] = v
	}
	return
}

// true则删掉,删掉表示后续不增加字段
func deepDeleteCheck(data interface{}, schema RepoSchemaEntry) bool {
	if schema.ValueType != PandoraTypeMap {
		return true
	}
	mval, ok := data.(map[string]interface{})
	if !ok {
		return true
	}
	if len(mval) > len(schema.Schema) {
		return false
	}
	for k, v := range mval {
		notfind := true
		for _, sv := range schema.Schema {
			if sv.Key == k {
				notfind = false
				if sv.ValueType == PandoraTypeMap && !deepDeleteCheck(v, sv) {
					return false
				}
			}
		}
		if notfind {
			return false
		}
	}
	return true
}

func copyData(d Data) Data {
	md := make(Data)
	for k, v := range d {
		md[k] = v
	}
	return md
}

func (c *Pipeline) generatePoint(repoName string, oldData Data, schemaFree bool) (point Point, err error) {
	data := copyData(oldData)
	point = Point{}
	c.repoSchemaMux.Lock()
	schemas := c.repoSchemas[repoName]
	c.repoSchemaMux.Unlock()
	if schemas == nil {
		schemas, err = c.getSchemas(repoName)
		if err != nil {
			reqe, ok := err.(*reqerr.RequestError)
			if ok && reqe.ErrorType != reqerr.NoSuchRepoError {
				return
			}
		}
	}
	c.repoSchemaMux.Lock()
	c.repoSchemas[repoName] = schemas
	c.repoSchemaMux.Unlock()
	for name, v := range schemas {
		value, ok := data[name]
		if !ok {
			//不存在，但是必填，需要加上默认值
			if v.Required {
				value = getDefault(v)
			} else {
				continue
			}
		}

		//对于没有autoupdate的情况就不delete了，节省CPU
		if schemaFree {
			if deepDeleteCheck(value, v) {
				delete(data, name)
			} else {
				//对于schemaFree，且检测发现有字段增加的data，continue掉，以免重复加入
				continue
			}
		}
		//加入point，要么已经delete，要么不schemaFree直接加入
		point.Fields = append(point.Fields, PointField{
			Key:   name,
			Value: value,
		})
	}
	/*
		data中剩余的值，但是在schema中不存在的，根据schemaFree增加。
	*/
	if schemaFree && len(data) > 0 {
		//defaultAll 为false时，过滤一批不要的
		valueType := getPandoraKeyValueType(data)
		if err = c.addRepoSchemas(repoName, valueType); err != nil {
			err = fmt.Errorf("updatePandora Repo error %v", err)
			return
		}
		for name, v := range data {
			point.Fields = append(point.Fields, PointField{
				Key:   name,
				Value: v,
			})
		}
	}
	return
}

type Schemas []RepoSchemaEntry

func (s Schemas) Len() int {
	return len(s)
}
func (s Schemas) Less(i, j int) bool {
	return s[i].Key < s[j].Key
}
func (s Schemas) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func mergePandoraSchemas(a, b []RepoSchemaEntry) (ret []RepoSchemaEntry, err error) {
	ret = make([]RepoSchemaEntry, 0)
	if a == nil && b == nil {
		return
	}
	if a == nil {
		ret = b
		return
	}
	if b == nil {
		ret = a
		return
	}
	sa := Schemas(a)
	sb := Schemas(b)
	sort.Sort(sa)
	sort.Sort(sb)
	i, j := 0, 0
	for {
		if i >= len(sa) {
			break
		}
		if j >= len(sb) {
			break
		}
		if sa[i].Key < sb[j].Key {
			ret = append(ret, sa[i])
			i++
			continue
		}
		if sa[i].Key > sb[j].Key {
			ret = append(ret, sb[j])
			j++
			continue
		}
		if sa[i].ValueType != sb[j].ValueType {
			err = fmt.Errorf("type conflict: key %v old type is <%v> want change to type <%v>", sa[i].Key, sa[i].ValueType, sb[j].ValueType)
			return
		}
		if sa[i].ValueType == PandoraTypeMap {
			if sa[i].Schema, err = mergePandoraSchemas(sa[i].Schema, sb[j].Schema); err != nil {
				return
			}
		}
		ret = append(ret, sa[i])
		i++
		j++
	}
	for ; i < len(sa); i++ {
		ret = append(ret, sa[i])
	}
	for ; j < len(sb); j++ {
		ret = append(ret, sb[j])
	}
	return
}

func (c *Pipeline) addRepoSchemas(repoName string, addSchemas map[string]RepoSchemaEntry) (err error) {

	var addScs, oldScs []RepoSchemaEntry
	for _, v := range addSchemas {
		addScs = append(addScs, v)
	}
	repo, err := c.GetRepo(&GetRepoInput{
		RepoName: repoName,
	})
	if err != nil {
		reqe, ok := err.(*reqerr.RequestError)
		if ok && reqe.ErrorType != reqerr.NoSuchRepoError {
			return
		}
	} else {
		oldScs = repo.Schema
	}
	schemas, err := mergePandoraSchemas(oldScs, addScs)
	if err != nil {
		return
	}
	if oldScs == nil {
		err = c.CreateRepo(&CreateRepoInput{
			RepoName: repoName,
			Schema:   schemas,
		})
	} else {
		err = c.UpdateRepo(&UpdateRepoInput{
			RepoName: repoName,
			Schema:   schemas,
		})
	}
	if err != nil {
		return
	}
	mpschemas := RepoSchema{}
	for _, sc := range schemas {
		mpschemas[sc.Key] = sc
	}
	c.repoSchemaMux.Lock()
	c.repoSchemas[repoName] = mpschemas
	c.repoSchemaMux.Unlock()
	return
}

/* Pandora类型的支持程度
PandoraTypeLong   ：全部支持
PandoraTypeFloat  ：全部支持
PandoraTypeString ：全部支持
PandoraTypeDate   ：支持rfc3339的string转化
PandoraTypeBool   ：全部支持
PandoraTypeArray  ：全部支持
PandoraTypeMap    ：全部支持
*/
func getPandoraKeyValueType(data Data) (valueType map[string]RepoSchemaEntry) {
	valueType = make(map[string]RepoSchemaEntry)
	for k, v := range data {
		switch nv := v.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			valueType[k] = formValueType(k, PandoraTypeLong)
		case float32, float64:
			valueType[k] = formValueType(k, PandoraTypeFloat)
		case bool:
			valueType[k] = formValueType(k, PandoraTypeBool)
		case json.Number:
			_, err := nv.Int64()
			if err == nil {
				valueType[k] = formValueType(k, PandoraTypeLong)
			} else {
				valueType[k] = formValueType(k, PandoraTypeFloat)
			}
		case map[string]interface{}:
			sc := formValueType(k, PandoraTypeMap)
			follows := getPandoraKeyValueType(Data(nv))
			for _, m := range follows {
				sc.Schema = append(sc.Schema, m)
			}
			valueType[k] = sc
		case []interface{}:
			sc := formValueType(k, PandoraTypeArray)
			if len(nv) > 0 {
				switch nnv := nv[0].(type) {
				case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
					sc.ElemType = PandoraTypeLong
				case float32, float64:
					sc.ElemType = PandoraTypeFloat
				case bool:
					sc.ElemType = PandoraTypeBool
				case json.Number:
					_, err := nnv.Int64()
					if err == nil {
						sc.ElemType = PandoraTypeLong
					} else {
						sc.ElemType = PandoraTypeFloat
					}
				case nil: // 不处理，不加入
				case string:
					sc.ElemType = PandoraTypeString
				default:
					sc.ValueType = PandoraTypeString
				}
				valueType[k] = sc
			}
			//对于里面没有元素的interface，不添加进去，因为无法判断类型
		case []int, []int8, []int16, []int32, []int64:
			sc := formValueType(k, PandoraTypeArray)
			sc.ElemType = PandoraTypeLong
			valueType[k] = sc
		case []float32, []float64:
			sc := formValueType(k, PandoraTypeArray)
			sc.ElemType = PandoraTypeFloat
			valueType[k] = sc
		case []bool:
			sc := formValueType(k, PandoraTypeArray)
			sc.ElemType = PandoraTypeBool
			valueType[k] = sc
		case []string:
			sc := formValueType(k, PandoraTypeArray)
			sc.ElemType = PandoraTypeBool
			valueType[k] = sc
		case nil: // 不处理，不加入
		case string:
			_, err := time.Parse(time.RFC3339, nv)
			if err == nil {
				valueType[k] = formValueType(k, PandoraTypeDate)
			} else {
				valueType[k] = formValueType(k, PandoraTypeString)
			}
		default:
			valueType[k] = formValueType(k, PandoraTypeString)
			log.Debugf("find undetected key(%v)-type(%v)", k, reflect.TypeOf(v))
		}
	}
	return
}

func formValueType(key, vtype string) RepoSchemaEntry {
	return RepoSchemaEntry{
		Key:       key,
		ValueType: vtype,
	}
}

func getDefault(t RepoSchemaEntry) (result interface{}) {
	switch t.ValueType {
	case PandoraTypeLong:
		result = 0
	case PandoraTypeFloat:
		result = 0.0
	case PandoraTypeString:
		result = ""
	case PandoraTypeDate:
		result = time.Now().Format(time.RFC3339Nano)
	case PandoraTypeBool:
		result = false
	case PandoraTypeMap:
		result = make(map[string]interface{})
	case PandoraTypeArray:
		switch t.ElemType {
		case PandoraTypeString:
			result = make([]string, 0)
		case PandoraTypeFloat:
			result = make([]float64, 0)
		case PandoraTypeLong:
			result = make([]int64, 0)
		case PandoraTypeBool:
			result = make([]bool, 0)
		}
	}
	return
}

func (c *Pipeline) getSchemaSorted(input *UpdateRepoInput) (err error) {
	repo, err := c.GetRepo(&GetRepoInput{
		RepoName: input.RepoName,
	})
	if err != nil {
		return
	}
	mschemas := make(map[string]RepoSchemaEntry)
	for _, sc := range input.Schema {
		mschemas[sc.Key] = sc
	}
	var schemas []RepoSchemaEntry
	for _, old := range repo.Schema {
		new, ok := mschemas[old.Key]
		if ok {
			schemas = append(schemas, new)
			delete(mschemas, old.Key)
		} else {
			schemas = append(schemas, old)
		}
	}
	for _, v := range mschemas {
		schemas = append(schemas, v)
	}
	input.Schema = schemas
	return
}
