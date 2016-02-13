package shell

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"
	"fmt"
	"encoding/hex"
)

type Token struct {
	Buf      string
	DataType uint
}

func (t *Token) ToMustValue() interface{} {
	v, err := t.ToValue()
	if err != nil {
		panic(err)
	}
	return v
}

func (t *Token) ToValue() (interface{}, error) {
	if t.DataType == DataTypeHexBytes {
		return nil, fmt.Errorf("unsuported type to convert value: %v", t.Buf)
	} else if t.DataType == DataTypeInt {
		v, err := strconv.ParseInt(t.Buf, 0, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	} else if t.DataType == DataTypeFloat {
		v, err := strconv.ParseFloat(t.Buf, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	} else if t.DataType == DataTypeString {
		return t.Buf, nil
	} else if t.DataType == DataTypeTerm {
		if t.Buf == "true" {
			return true, nil
		} else if t.Buf == "false" {
			return false, nil
		} else {
			return nil, fmt.Errorf("unsuported type to convert value: %v", t.Buf)
		}
	} else {
		return nil, fmt.Errorf("unknown data type '%s'", t.Buf)
	}
}


func (t *Token) ToBytes() ([]byte, error) {
	if t.DataType == DataTypeHexBytes {
		b ,err := hex.DecodeString(t.Buf[2:])
		if err != nil {
			return nil, err
		}

		return b, nil
	} else if t.DataType == DataTypeInt {
		v, err := strconv.ParseInt(t.Buf, 0, 64)
		if err != nil {
			return nil, err
		}

		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v))

		return b, nil
	} else if t.DataType == DataTypeFloat {
		v, err := strconv.ParseFloat(t.Buf, 64)
		if err != nil {
			return nil, err
		}
		bits := math.Float64bits(v)
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, bits)

		return b, nil
	} else if t.DataType == DataTypeString {
		return []byte(t.Buf), nil
	} else if t.DataType == DataTypeTerm {
		return nil, fmt.Errorf("term '%s' can not be converted to the bytes slice", t.Buf)
	} else {
		return nil, fmt.Errorf("unknown data type '%s'", t.Buf)
	}
}

func (t *Token) ToMustBytes() []byte {
	b, err := t.ToBytes()
	if err != nil {
		panic(err)
	}
	return b
}

const (
	DataTypeHexBytes = 0
	DataTypeInt    = 1
	DataTypeFloat  = 2
	DataTypeString = 3
	DataTypeTerm   = 4
)

// tokenize logic was inspired https://github.com/mattn/go-shellwords

// tokenize
func Tokenize(line string) ([]*Token, error) {
	line = strings.TrimSpace(line)

	tokens := []*Token{}
	buf := ""
	var escaped, doubleQuoted, singleQuoted bool
	var usingQuoted bool

	backtick := ""

	for _, r := range line {
		if escaped {
			buf += string(r)
			escaped = false
			continue
		}

		if r == '\\' {
			if singleQuoted {
				buf += string(r)
			} else {
				escaped = true
			}
			continue
		}

		if isSpace(r) {
			if singleQuoted || doubleQuoted {
				buf += string(r)
				backtick += string(r)
			} else if buf != "" {
				token := &Token{
					Buf: buf,
				}

				if usingQuoted {
					token.DataType = DataTypeString
				} else {
					if strings.HasPrefix(buf, "0x") {
						token.DataType = DataTypeHexBytes
					} else {
						_, err := strconv.ParseInt(buf, 0, 64)
						if err == nil {
							token.DataType = DataTypeInt
						} else {
							_, err := strconv.ParseFloat(buf, 64)
							if err == nil {
								token.DataType = DataTypeFloat
							} else {
								token.DataType = DataTypeTerm
							}
						}
					}
				}

				tokens = append(tokens, token)

				buf = ""
				usingQuoted = false
			}
			continue
		}

		switch r {
		case '"':
			if !singleQuoted {
				doubleQuoted = !doubleQuoted

				usingQuoted = true

				continue
			}
		case '\'':
			if !doubleQuoted {
				singleQuoted = !singleQuoted

				usingQuoted = true

				continue
			}
		}

		buf += string(r)
	}

	if buf != "" {
		token := &Token{
			Buf: buf,
		}

		if usingQuoted {
			token.DataType = DataTypeString
		} else {
			if strings.HasPrefix(buf, "0x") {
				token.DataType = DataTypeHexBytes
			} else {
				_, err := strconv.ParseInt(buf, 0, 64)
				if err == nil {
					token.DataType = DataTypeInt
				} else {
					_, err := strconv.ParseFloat(buf, 64)
					if err == nil {
						token.DataType = DataTypeFloat
					} else {
						token.DataType = DataTypeTerm
					}
				}
			}
		}

		tokens = append(tokens, token)
	}

	if escaped || singleQuoted || doubleQuoted {
		return nil, errors.New("invalid line string")
	}

	return tokens, nil
}

func isSpace(r rune) bool {
	switch r {
	case ' ', '\t', '\r', '\n':
		return true
	}
	return false
}
