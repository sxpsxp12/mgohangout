package filter

import (
	"encoding/json"
	"github.com/childe/gohangout/field_setter"
	"github.com/childe/gohangout/topology"
	"github.com/childe/gohangout/value_render"
	"github.com/golang/glog"
)

func (f *JsonObjectFlatFilter) SetBelongTo(next topology.Processor) {
	f.next = next
}

type JsonObjectFlatFilter struct {
	config map[interface{}]interface{}
	src    value_render.ValueRender

	target    field_setter.FieldSetter
	overwrite bool

	next topology.Processor
}

func init() {
	Register("JsonObjectFlat", newObjectFlatFilter)
}

func newObjectFlatFilter(config map[interface{}]interface{}) topology.Filter {
	p := &JsonObjectFlatFilter{
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
		glog.Fatal("fields [target] must be set in JsonObjectFlatFilter filter")
	}

	return p
}

func (f *JsonObjectFlatFilter) Filter(event map[string]interface{}) (map[string]interface{}, bool) {
	objects, ok := f.src.Render(event).(map[string]interface{})
	if !ok {
		glog.Error("JsonObjectFlatFilter src parse failed")
		return event, false
	}

	//准备深拷贝event
	eventStr, err := json.Marshal(event)
	if err != nil {
		return event, false
	}

	for k, part := range objects {
		newEvent := make(map[string]interface{})
		if json.Unmarshal(eventStr, &newEvent) != nil {
			return event, false
		}

		//flat object
		if newPart, ok := part.(map[string]interface{}); ok {
			newPart["flat_key"] = k
			f.target.SetField(newEvent, newPart, "", f.overwrite)
		} else {
			f.target.SetField(newEvent, map[string]interface{}{
				k: part,
			}, "", f.overwrite)
		}

		f.next.Process(newEvent)
	}
	//原event不处理了
	return nil, true
}
