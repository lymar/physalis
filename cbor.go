package physalis

import "github.com/fxamacker/cbor/v2"

var cborEncMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.Time = cbor.TimeRFC3339Nano
	em, err := opts.EncMode()
	if err != nil {
		panic(err)
	}
	cborEncMode = em
}

func CBORMarshal(v any) ([]byte, error) {
	return cborEncMode.Marshal(v)
}

func CBORUnmarshal(data []byte, v any) error {
	return cbor.Unmarshal(data, v)
}
