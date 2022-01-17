package filter

import (
	"encoding/json"
	"strconv"
	"testing"
)

func TestIntConvert(t *testing.T) {
	type testCase struct {
		v    interface{}
		want interface{}
		err  error
	}

	convert := &IntConverter{}

	cases := []testCase{
		{
			json.Number("1"), int64(1), nil,
		},
		{
			"1", int64(1), nil,
		},
		{
			1, nil, ErrConvertUnknownFormat,
		},
		{
			"12345678901234567890", int64(9223372036854775807), &strconv.NumError{Func: "ParseInt", Num: "12345678901234567890", Err: strconv.ErrRange},
		},
	}

	for _, c := range cases {
		ans, err := convert.convert(c.v)
		if ans != c.want {
			t.Errorf("want %v, got %v", c.want, ans)
		}

		if err == nil {
			if c.err != nil {
				t.Errorf("want %v, got %v", c.err, err)
			}
		} else {
			if c.err == nil || err.Error() != c.err.Error() {
				t.Errorf("want %v, got %v", c.err, err)
			}
		}
	}
}

func TestSettoIfNil(t *testing.T) {
	config := make(map[interface{}]interface{})
	fields := make(map[interface{}]interface{})
	fields["timeTaken"] = map[interface{}]interface{}{
		"to":           "float",
		"setto_if_nil": 0.0,
	}
	config["fields"] = fields
	f := BuildFilter("Convert", config)
	event := map[string]interface{}{}

	event, ok := f.Filter(event)
	t.Log(event)

	if ok == false {
		t.Error("ConvertFilter fail")
	}

	if event["timeTaken"].(float64) != 0.0 {
		t.Error("timeTaken convert error")
	}
}

func TestConvertFilter(t *testing.T) {
	config := make(map[interface{}]interface{})
	fields := make(map[interface{}]interface{})
	fields["id"] = map[interface{}]interface{}{
		"to":            "uint",
		"setto_if_fail": 0,
	}
	fields["responseSize"] = map[interface{}]interface{}{
		"to":            "int",
		"setto_if_fail": 0,
	}
	fields["timeTaken"] = map[interface{}]interface{}{
		"to":             "float",
		"remove_if_fail": true,
	}
	// add to string test case
	fields["toString"] = map[interface{}]interface{}{
		"to":             "string",
		"remove_if_fail": true,
	}
	config["fields"] = fields
	f := BuildFilter("Convert", config)

	case1 := map[string]int{"a": 5, "b": 7}
	event := map[string]interface{}{
		"id":           "12345678901234567890",
		"responseSize": "10",
		"timeTaken":    "0.010",
		"toString":     case1,
	}
	t.Log(event)

	event, ok := f.Filter(event)
	t.Log(event)

	if ok == false {
		t.Error("ConvertFilter fail")
	}

	if event["id"].(uint64) != 12345678901234567890 {
		t.Error("id should be 12345678901234567890")
	}
	if event["responseSize"].(int64) != 10 {
		t.Error("responseSize should be 10")
	}
	if event["timeTaken"].(float64) != 0.01 {
		t.Error("timeTaken should be 0.01")
	}
	if event["toString"].(string) != "{\"a\":5,\"b\":7}" {
		t.Error("toString is unexpected")
	}
	event = map[string]interface{}{
		"responseSize": "10.1",
		"timeTaken":    "abcd",
		"toString":     "huangjacky",
	}
	t.Log(event)

	event, ok = f.Filter(event)
	t.Log(event)

	if ok == false {
		t.Error("ConvertFilter fail")
	}

	if event["responseSize"].(int) != 0 {
		t.Error("responseSize should be 0")
	}
	if event["timeTaken"] != nil {
		t.Error("timeTaken should be nil")
	}
	if event["toString"].(string) != "huangjacky" {
		t.Error("toString should be huangjacky")
	}
}
