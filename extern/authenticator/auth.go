package authenticator

//身份验证
var (
	Table = []string{
		"A", "B", "C", "D", "E", "F", "G", "H", // 7
		"I", "J", "K", "L", "M", "N", "O", "P", // 15
		"Q", "R", "S", "T", "U", "V", "W", "X", // 23
		"Y", "Z", "2", "3", "4", "5", "6", "7", // 31
		"=", // 填充字符 padding char
	}

	Token = ""
	Zckey = []byte("qwertyuiopasdfgh")
	Zciv  = []byte("jkl;'zxcvbnm,./?")
)
