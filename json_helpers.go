package fluxaorm

import jsoniter "github.com/json-iterator/go"

func JsonMarshalToString(v any) (string, error) {
	return jsoniter.ConfigFastest.MarshalToString(v)
}

func JsonUnmarshalFromString(data string, v any) error {
	return jsoniter.ConfigFastest.UnmarshalFromString(data, v)
}
