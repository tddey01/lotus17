package server_c2

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
)

type Respones struct {
	Code int
	Data interface{}
}

func NewRespones(code int) *Respones {
	return &Respones{Code: code}
}
func (r *Respones) Result(w http.ResponseWriter, data interface{}) {
	fmt.Println("Result.data:", reflect.Indirect(reflect.ValueOf(data)).Type())
	r.Data = data
	if _, ok := data.([]byte); ok {
		r.Data = base64.StdEncoding.EncodeToString(data.([]byte))
	}
	if _, ok := data.(string); ok {
		r.Data = base64.StdEncoding.EncodeToString([]byte(data.(string)))
	}
	da, err := json.Marshal(*r)
	if err != nil {
		fmt.Println("Result:", err.Error())
		return
	}
	w.Write(da)
	w.WriteHeader(r.Code)
}
func (r *Respones) ResultError(w http.ResponseWriter, err error) {
	w.Write([]byte(err.Error()))
	w.WriteHeader(r.Code)
}
func (r *Respones) Unmarshal(rd io.Reader) error {
	body, err := ioutil.ReadAll(rd)
	if err != nil {
		return err
	}
	if _, ok := r.Data.(string); ok {
		r.Data = base64.StdEncoding.EncodeToString([]byte(r.Data.(string)))
	}
	return json.Unmarshal(body, r)
}
