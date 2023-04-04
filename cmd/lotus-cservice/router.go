package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"github.com/filecoin-project/lotus/extern/server-c2"
	"net/http"
	"reflect"
	"time"
)

const (
	API_KEY           = "token"
	TOKEN_KEY         = "yungojs"
	TOKEN_EFFECT_TIME = 24 * time.Hour
)

func Server(host string) error {

	web := Init()
	web.addMiddleWare(checkHandleWare)
	web.addFn("/connectserver", ConnectServer)     //连接C2集群服务
	web.addFn("/runcommit2", RunCommit2)           //执行C2
	web.addFn("/getcommit2", GetCommit2)           //获取C2结果
	web.addFn("/gpucount", GpuCount)               //获取GUP数量
	web.addFn("/completecommit2", CompleteCommit2) //完成C2

	http.HandleFunc("/", web.ServerHttp)
	return http.ListenAndServe(":"+host, nil)
}
func Init() *WebRouter {
	return &WebRouter{
		Rotuer: make(map[string]HandleFunc),
	}
}
func (w *WebRouter) addMiddleWare(middleware MiddleFunc) {
	w.MiddleWares = append(w.MiddleWares, middleware)
}
func (w *WebRouter) addRotuer(rotuer string, handle HandleFunc) {
	w.Rotuer[rotuer] = handle
}
func (w *WebRouter) addFn(rotuer string, fn HandleFunc) {
	for i := len(w.MiddleWares) - 1; i >= 0; i-- {
		//fn(nil)
		fn = w.MiddleWares[i](fn)
	}
	w.addRotuer(rotuer, fn)
}
func (wr *WebRouter) ServerHttp(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	ctx := Context{w, r, make(map[string]interface{}), nil}
	if wr.Rotuer[path] != nil {
		wr.Rotuer[path](&ctx)
	} else {
		ctx.Result(http.StatusNotFound, "404 not fond")
	}
}

type WebRouter struct {
	MiddleWares []MiddleFunc
	Rotuer      map[string]HandleFunc
}
type Context struct {
	response http.ResponseWriter
	request  *http.Request
	keys     map[string]interface{}
	errs     error
}

func (ctx *Context) Set(key string, value interface{}) {
	ctx.keys[key] = value
}
func (ctx *Context) Get(key string) interface{} {
	return ctx.keys[key]
}
func (ctx *Context) Result(code int, data interface{}) {
	var res server_c2.Respones
	res.Data = data
	res.Code = code
	//if _, ok := data.([]byte); ok {
	//	res.Data = base64.StdEncoding.EncodeToString(data.([]byte))
	//}
	//if _, ok := data.(string); ok {
	//	res.Data = base64.StdEncoding.EncodeToString([]byte(data.(string)))
	//}
	da, err := json.Marshal(res)
	if err != nil {
		fmt.Println("Result:", err.Error())
	}
	ctx.response.WriteHeader(code)
	ctx.response.Write(da)
}

type HandleFunc func(ctx *Context)
type MiddleFunc func(next HandleFunc) HandleFunc

func checkHandleWare(next HandleFunc) HandleFunc {
	return func(ctx *Context) {
		if ctx.errs != nil {
			return
		}
		token := ctx.request.Header.Get(API_KEY)
		mid, err := checkToken(token, TOKEN_KEY)
		if err != nil {
			ctx.errs = err
			server_c2.NewRespones(500).ResultError(ctx.response, err)
			return
		}
		ctx.Set("mid", mid)
		next(ctx)
	}
}

func checkToken(token string, key string) (string, error) {
	token1, err := jwt.Parse(token, func(t *jwt.Token) (interface{}, error) {
		if jwt.GetSigningMethod("HS256") != t.Method {
			return nil, errors.New("算法不对")
		}

		return []byte(key), nil
	})
	if err != nil {
		return "", err
	}
	claims := token1.Claims.(jwt.MapClaims)
	if _, ok := claims["miner_id"].(string); !ok {
		return "", errors.New("Miner_id类型有误")
	}
	if _, ok := claims["exp"].(float64); !ok {
		val := reflect.ValueOf(claims["exp"])
		typ := reflect.Indirect(val).Type()
		fmt.Println("exp:", typ.String(), ",value:", claims["exp"])
		return "", errors.New("exp类型有误")
	}
	if time.Now().Unix() > int64(claims["exp"].(float64)) {
		return "", errors.New("token:" + token + "已过期")
	}

	return claims["miner_id"].(string), nil
}
