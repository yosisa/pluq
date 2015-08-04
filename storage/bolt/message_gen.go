package bolt

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Message) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var isz uint32
	isz, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for isz > 0 {
		isz--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "body":
			z.Body, err = dc.ReadBytes(z.Body)
			if err != nil {
				return
			}
		case "meta":
			var msz uint32
			msz, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Meta == nil && msz > 0 {
				z.Meta = make(map[string]interface{}, msz)
			} else if len(z.Meta) > 0 {
				for key, _ := range z.Meta {
					delete(z.Meta, key)
				}
			}
			for msz > 0 {
				msz--
				var xvk string
				var bzg interface{}
				xvk, err = dc.ReadString()
				if err != nil {
					return
				}
				bzg, err = dc.ReadIntf()
				if err != nil {
					return
				}
				z.Meta[xvk] = bzg
			}
		case "ct":
			z.ContentType, err = dc.ReadString()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *Message) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "body"
	err = en.Append(0x83, 0xa4, 0x62, 0x6f, 0x64, 0x79)
	if err != nil {
		return err
	}
	err = en.WriteBytes(z.Body)
	if err != nil {
		return
	}
	// write "meta"
	err = en.Append(0xa4, 0x6d, 0x65, 0x74, 0x61)
	if err != nil {
		return err
	}
	err = en.WriteMapHeader(uint32(len(z.Meta)))
	if err != nil {
		return
	}
	for xvk, bzg := range z.Meta {
		err = en.WriteString(xvk)
		if err != nil {
			return
		}
		err = en.WriteIntf(bzg)
		if err != nil {
			return
		}
	}
	// write "ct"
	err = en.Append(0xa2, 0x63, 0x74)
	if err != nil {
		return err
	}
	err = en.WriteString(z.ContentType)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Message) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "body"
	o = append(o, 0x83, 0xa4, 0x62, 0x6f, 0x64, 0x79)
	o = msgp.AppendBytes(o, z.Body)
	// string "meta"
	o = append(o, 0xa4, 0x6d, 0x65, 0x74, 0x61)
	o = msgp.AppendMapHeader(o, uint32(len(z.Meta)))
	for xvk, bzg := range z.Meta {
		o = msgp.AppendString(o, xvk)
		o, err = msgp.AppendIntf(o, bzg)
		if err != nil {
			return
		}
	}
	// string "ct"
	o = append(o, 0xa2, 0x63, 0x74)
	o = msgp.AppendString(o, z.ContentType)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Message) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var isz uint32
	isz, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for isz > 0 {
		isz--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "body":
			z.Body, bts, err = msgp.ReadBytesBytes(bts, z.Body)
			if err != nil {
				return
			}
		case "meta":
			var msz uint32
			msz, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Meta == nil && msz > 0 {
				z.Meta = make(map[string]interface{}, msz)
			} else if len(z.Meta) > 0 {
				for key, _ := range z.Meta {
					delete(z.Meta, key)
				}
			}
			for msz > 0 {
				var xvk string
				var bzg interface{}
				msz--
				xvk, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				bzg, bts, err = msgp.ReadIntfBytes(bts)
				if err != nil {
					return
				}
				z.Meta[xvk] = bzg
			}
		case "ct":
			z.ContentType, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				return
			}
		}
	}
	o = bts
	return
}

func (z *Message) Msgsize() (s int) {
	s = 1 + 5 + msgp.BytesPrefixSize + len(z.Body) + 5 + msgp.MapHeaderSize
	if z.Meta != nil {
		for xvk, bzg := range z.Meta {
			_ = bzg
			s += msgp.StringPrefixSize + len(xvk) + msgp.GuessSize(bzg)
		}
	}
	s += 3 + msgp.StringPrefixSize + len(z.ContentType)
	return
}
