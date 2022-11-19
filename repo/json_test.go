package repo

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

type Place struct {
	Lat float32 `json:"lat"`
	Lon float32 `json:"lon"`
}

func TestJson(t *testing.T) {
	newPlace := &Place{Lat: 15.42, Lon: 16.23}
	placeBytes, err := json.Marshal(newPlace)
	fmt.Println(string(placeBytes))
	if err == nil {
		var reCast *Place
		if err := json.Unmarshal(placeBytes, &reCast); err == nil {
			oldPlace := &Place{}
			rvOld := reflect.Indirect(reflect.ValueOf(oldPlace))
			rvOld.Set(reflect.Indirect(reflect.ValueOf(reCast)))
			fmt.Println(oldPlace)
		} else {
			fmt.Println(err)
		}
	}
}
