package filter

import (
	"encoding/json"
	"github.com/childe/gohangout/field_setter"
	"github.com/childe/gohangout/topology"
	"github.com/childe/gohangout/value_render"
	"github.com/golang/glog"
)

func (f *JsonArrayFlatFilter) SetBelongTo(next topology.Processor) {
	f.next = next
}

type JsonArrayFlatFilter struct {
	config map[interface{}]interface{}
	src    value_render.ValueRender

	target    field_setter.FieldSetter
	overwrite bool

	next topology.Processor
}

func init() {
	Register("JsonArrayFlat", newArrayFlatFilter)
}

func newArrayFlatFilter(config map[interface{}]interface{}) topology.Filter {
	p := &JsonArrayFlatFilter{
		config:    config,
		overwrite: true,
	}

	if overwrite, ok := config["overwrite"]; ok {
		p.overwrite = overwrite.(bool)
	}

	if src, ok := config["src"]; ok {
		p.src = value_render.GetValueRender2(src.(string))

	} else {
		p.src = value_render.GetValueRender2("message")
	}

	if target, ok := config["target"]; ok {
		fieldSetter := field_setter.NewFieldSetter(target.(string))
		if fieldSetter == nil {
			glog.Fatalf("could build field setter from %s", target.(string))
		}
		p.target = fieldSetter
	} else {
		glog.Fatal("fields [target] must be set in JsonArrayFlatFilter filter")
	}

	return p
}

func (f *JsonArrayFlatFilter) Filter(event map[string]interface{}) (map[string]interface{}, bool) {
	arrays, ok := f.src.Render(event).([]interface{})
	if !ok {
		glog.Error("JsonArrayFlatFilter src parse failed")
		return event, false
	}

	//准备深拷贝event
	eventStr, err := json.Marshal(event)
	if err != nil {
		return event, false
	}

	for _, part := range arrays {
		newEvent := make(map[string]interface{})
		if json.Unmarshal(eventStr, &newEvent) != nil {
			return event, false
		}

		//flat array
		f.target.SetField(newEvent, part, "", f.overwrite)

		f.next.Process(newEvent)
	}
	//原event不处理了
	return nil, true
}
