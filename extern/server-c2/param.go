package server_c2

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
)
type Param map[string]interface{}
func NewParam(data []byte) (Param, error) {
	param := make(Param)
	err := json.Unmarshal(data, &param)
	if err != nil {
		log.Println("反序列化错误：", err.Error())
		return nil, err
	}
	return param, nil
}

func (s Param) Marshal() io.Reader {
	b, err := json.Marshal(s)
	if err != nil {
		log.Println("序列化错误：", err.Error())
		return nil
	}
	return bytes.NewReader(b)
}

func (s Param) UnMarshal(data []byte) error {
	err := json.Unmarshal(data, &s)
	if err != nil {
		log.Println("反序列化错误：", err.Error())
		return nil
	}
	return json.Unmarshal(data, &s)
}