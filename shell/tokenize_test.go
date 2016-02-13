package shell

import (
	"testing"
)

func TestTokenize(t *testing.T) {
	tokens, err := Tokenize("aaa 123 123.45 'ccc'")
	if err != nil {
		t.Errorf("Got a err: %v", err)
	}

	if tokens[0].Buf != "aaa" || tokens[0].DataType != DataTypeTerm {
		t.Errorf("invaid spliting result %s dataType: %d", tokens[0].Buf, tokens[0].DataType)
	}
	if tokens[1].Buf != "123" || tokens[1].DataType != DataTypeInt {
		t.Errorf("invaid spliting result %s dataType: %d", tokens[1].Buf, tokens[1].DataType)
	}
	if tokens[2].Buf != "123.45" || tokens[2].DataType != DataTypeFloat {
		t.Errorf("invaid spliting result %s dataType: %d", tokens[2].Buf, tokens[2].DataType)
	}
	if tokens[3].Buf != "ccc" || tokens[3].DataType != DataTypeString {
		t.Errorf("invaid spliting result %s dataType: %d", tokens[3].Buf, tokens[3].DataType)
	}

	tokens, err = Tokenize("0x0f01")
	if err != nil {
		t.Errorf("Got a err: %v", err)
	}

	if tokens[0].ToMustBytes()[0] != byte(15) {
		t.Errorf("invaid result %s dataType: %d", tokens[0].Buf, tokens[0].DataType)
	}

	if tokens[0].ToMustBytes()[1] != byte(01) {
		t.Errorf("invaid result %s dataType: %d", tokens[0].Buf, tokens[0].DataType)
	}
}

//
//func TestTokenize(t *testing.T) {
//	str, err := tokenize("aaa bbb ccc")
//	if err != nil {
//		t.Errorf("Got a err: %v", err)
//	}
//
//	if str[0] != "aaa" {
//		t.Errorf("invaid spliting result %s", str[0])
//	}
//	if str[1] != "bbb" {
//		t.Errorf("invaid spliting result %s", str[0])
//	}
//	if str[2] != "ccc" {
//		t.Errorf("invaid spliting result %s", str[0])
//	}
//
//	str, err = tokenize("aaa    bbb   ccc")
//	if err != nil {
//		t.Errorf("Got a err: %v", err)
//	}
//
//	if str[0] != "aaa" {
//		t.Errorf("invaid spliting result %s", str[0])
//	}
//	if str[1] != "bbb" {
//		t.Errorf("invaid spliting result %s", str[0])
//	}
//	if str[2] != "ccc" {
//		t.Errorf("invaid spliting result %s", str[0])
//	}
//
//	str, err = tokenize("aaa    'bbb'   '  ccc'")
//	if err != nil {
//		t.Errorf("Got a err: %v", err)
//	}
//
//	if str[0] != "aaa" {
//		t.Errorf("invaid spliting result %s", str[0])
//	}
//	if str[1] != "bbb" {
//		t.Errorf("invaid spliting result %s", str[0])
//	}
//	if str[2] != "  ccc" {
//		t.Errorf("invaid spliting result %s", str[0])
//	}
//}
