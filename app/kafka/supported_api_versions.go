package kafka

import (
	protocol "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

var supportedAPIs = []int16{18, 75} // Supported APIs
var supportedAPIVersionMap = map[int16][2]int16{
	18: {0, 4},
	75: {0, 2},
}

func HandleSupportedVersions(requestHeader RequestHeader, encoder *protocol.Encoder) {
	if contains(supportedAPIs, int16(requestHeader.ApiKey)) {
		supportedAPIVersions := supportedAPIVersionMap[int16(requestHeader.ApiKey)]
		if requestHeader.ApiVersion >= supportedAPIVersions[0] &&
			requestHeader.ApiVersion <= supportedAPIVersions[1] {
			encoder.Int16(0) // error_code: NO_ERROR
			return
		} else {
			encoder.Int16(35) // error_code: UNSUPPORTED_VERSION
			return
		}
	}

}
