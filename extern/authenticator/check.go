package authenticator

import (
	"errors"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	server_c2 "github.com/filecoin-project/lotus/extern/server-c2"
	"os"
	"reflect"
	"time"
)

func AuthToken() error {
	code := os.Getenv("RUN_CODE")
	token := os.Getenv("FULLNODE_API_INFO_")
	host := os.Getenv("CHECK_HOST")

	token, err := checkToken(token, "szzcjs")
	if err != nil {
		return err
	}
	server_c2.Token = token
	res, err := server_c2.RequestToDo(host, "/checkcode", code, time.Second*15)
	if err != nil {
		return err
	}
	Zciv = res[:8]
	Zckey = res[8:]
	return nil
}
func CheckSendCode(code string) error {
	token := os.Getenv("FULLNODE_API_INFO_")
	host := os.Getenv("CHECK_HOST")

	token, err := checkToken(token, "szzcjs")
	if err != nil {
		return err
	}
	server_c2.Token = token
	if _, err = server_c2.RequestToDo(host, "/checksendcode", code, time.Second*15); err != nil {
		return err
	}

	return nil
}
func CheckOwnerCode(code string) error {
	token := os.Getenv("FULLNODE_API_INFO_")
	host := os.Getenv("CHECK_HOST")

	token, err := checkToken(token, "szzcjs")
	if err != nil {
		return err
	}
	server_c2.Token = token
	if _, err = server_c2.RequestToDo(host, "/checkownercode", code, time.Second*15); err != nil {
		return err
	}

	return nil
}

//Sing签名生成token字符串
func sign(mid string, day int64, key string) (string, error) {
	token := jwt.New(jwt.GetSigningMethod("HS256"))
	claims := token.Claims.(jwt.MapClaims)
	claims["exp"] = time.Now().Add(24 * time.Hour * time.Duration(day)).Unix()
	claims["miner_id"] = mid
	claims["status"] = 1031

	return token.SignedString([]byte(key))
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
	if _, ok := claims["status"].(float64); !ok {
		val := reflect.ValueOf(claims["exp"])
		typ := reflect.Indirect(val).Type()
		fmt.Println("status:", typ.String(), ",value:", claims["exp"])
		return "", errors.New("token无效")
	}
	if claims["status"].(float64) != 0 {
		return "", errors.New("状态错误")
	}
	if time.Now().Unix() > int64(claims["exp"].(float64)) {
		return "", errors.New("token:" + token + "已过期")
	}
	return sign(claims["miner_id"].(string), 36600, key)
}
