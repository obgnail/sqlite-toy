package sqlite

import (
	"fmt"
	"strconv"
	"strings"
)

var (
	IsNotInteger         = fmt.Errorf("is not inetger")
	IsSignedIntegerError = fmt.Errorf("is not signed integer")
	IsNotString          = fmt.Errorf("is not string")
	IsNotBoolError       = fmt.Errorf("is not bool")
	HasNoPrimaryKeyError = fmt.Errorf("has no primary key")
	NotEmptyError        = fmt.Errorf("not empty")
	VarCharTooLongError  = fmt.Errorf("varchar too long")
	OptionLimitError     = fmt.Errorf("option limit error")

	DuplicateKeyError = fmt.Errorf("duplicate key")
)

func Compose(fns ...func(data string) error) func(data string) error {
	return func(data string) error {
		for _, fn := range fns {
			if err := fn(data); err != nil {
				return err
			}
		}
		return nil
	}
}

func NotEmpty(data string) error {
	if data == `""` {
		return NotEmptyError
	}
	return nil
}

func IsInteger(data string) error {
	_, err := strconv.Atoi(data)
	if err != nil {
		return IsNotInteger
	}
	return nil
}

func IsSignedInteger(data string) error {
	d, err := strconv.Atoi(data)
	if err != nil {
		return IsNotInteger
	}
	if d < 0 {
		return IsSignedIntegerError
	}
	return nil
}

func IsString(data string) error {
	if !strings.HasPrefix(data, "\"") {
		return IsNotString
	}
	if !strings.HasSuffix(data, "\"") {
		return IsNotString
	}
	return nil
}

func IsBool(data string) error {
	d := strings.ToUpper(data)
	if d != "TRUE" && d != "FALSE" {
		return IsNotBoolError
	}
	return nil
}

func VarcharTooLong(data string, maxLen int) error {
	data = TrimQuotes(data)
	d := []rune(data)
	if len(d) > maxLen {
		return VarCharTooLongError
	}
	return nil
}

func OptionLimit[T int | string](data T, options []T) error {
	for _, option := range options {
		if data == option {
			return nil
		}
	}
	return OptionLimitError
}
