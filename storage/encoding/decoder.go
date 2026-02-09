// langgraphdemogo/lance/encoding/decoder.go
package encoding

import (
	lerrors "github.com/wzqhbustb/vego/storage/errors"
	"github.com/wzqhbustb/vego/storage/format"
)

// GetDecoder returns a Decoder for the given encoding type.
// Returns (nil, nil) for plain encoding (no decoding needed).
func GetDecoder(encodingType format.EncodingType) (Decoder, error) {
	switch encodingType {
	case format.EncodingPlain:
		return nil, nil
	case format.EncodingZstd:
		return NewZstdDecoder()
	case format.EncodingBitPacked:
		return NewBitPackingDecoder(), nil
	case format.EncodingRLE:
		return NewRLEDecoder(), nil
	case format.EncodingDictionary:
		return NewDictionaryDecoder(), nil
	case format.EncodingBSSEncoding:
		return NewBSSDecoder(), nil
	case format.EncodingDelta:
		return nil, lerrors.New(lerrors.ErrNotSupported).
			Op("get_decoder").
			Context("encoding", "delta").
			Build()
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("get_decoder").
			Context("encoding", encodingType.String()).
			Build()
	}
}
