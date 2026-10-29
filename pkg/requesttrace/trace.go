// Package requesttrace carries correlation metadata outside model inputs.
package requesttrace

import "net/http"

const (
	RequestIDHeader = "X-Request-ID"
	BatchIDHeader   = "X-Rank-Batch-ID"
)

// Trace identifies one algorithm batch within a recommendation request.
// It is a value, so concurrent requests never mutate shared client headers.
type Trace struct {
	RequestID string
	BatchID   string
}

// ValidID permits only bounded, single-line ASCII correlation identifiers.
func ValidID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '.' || c == ':' || c == '-') {
			return false
		}
	}
	return true
}

// LogValue returns a safe identifier, or the explicit missing-value marker.
func LogValue(value string) string {
	if ValidID(value) {
		return value
	}
	return "-"
}

// SetHeaders applies only validated per-call metadata. Explicit traced calls
// replace any static correlation headers rather than inheriting another ID.
// Legacy, untraced callers do not call this method.
func (t Trace) SetHeaders(headers http.Header) {
	headers.Del(RequestIDHeader)
	headers.Del(BatchIDHeader)
	if ValidID(t.RequestID) {
		headers.Set(RequestIDHeader, t.RequestID)
	}
	if ValidID(t.BatchID) {
		headers.Set(BatchIDHeader, t.BatchID)
	}
}
