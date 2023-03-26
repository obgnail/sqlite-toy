package sqlite

import (
	"strconv"
	"strings"
)

func TrimQuotes(data string) string {
	return strings.Trim(data, "\"")
}

func StringFormatter(data string) interface{} {
	return TrimQuotes(data)
}

func integerFormatter(data string) interface{} {
	d, err := strconv.Atoi(data)
	if err != nil {
		panic(err)
	}
	return d
}

func BoolFormatter(data string) interface{} {
	d := strings.ToUpper(data)
	switch d {
	case "TRUE":
		return true
	case "FALSE":
		return false
	default:
		panic("data is not bool")
	}
}
