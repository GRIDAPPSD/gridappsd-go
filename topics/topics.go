// Package topics provides GOSS destination name helpers and well-known topic constants.
//
// NormalizeDestination implements the queue-prepend rule from goss.py:415-417:
// any destination not already prefixed with /topic/, /queue/, or /temp-queue/
// is prefixed with /queue/. This rule is applied on Subscribe, Send, and the
// GetResponse request destination inside fieldbus and internal/reqresp.
package topics

import "strings"

// Well-known GOSS platform destination constants.
// These stable strings may be referenced without a query API.
const (
	// FieldBusInput is the field-bus input topic for field-side messages.
	FieldBusInput = "/topic/goss.gridappsd.field.input"
	// FieldBusOutput is the field-bus output topic for simulation-side messages.
	FieldBusOutput = "/topic/goss.gridappsd.field.output"

	// Platform service request destinations (read-only constants; the methods
	// that use them land in phase 3+).
	Blazegraph     = "goss.gridappsd.process.request.data.powergridmodel"
	Logs           = "goss.gridappsd.process.request.log"
	Timeseries     = "goss.gridappsd.process.request.data.timeseries"
	Config         = "goss.gridappsd.process.request.config"
	PlatformStatus = "goss.gridappsd.process.request.status.platform"
)

// NormalizeDestination applies the queue-prepend rule.
//
// A destination that already starts with /topic/, /queue/, or /temp-queue/ is
// returned unchanged. Any other value is prefixed with /queue/. This mirrors
// goss.py:415-417 (CallbackRouter.add_callback).
func NormalizeDestination(dest string) string {
	if strings.HasPrefix(dest, "/topic/") ||
		strings.HasPrefix(dest, "/queue/") ||
		strings.HasPrefix(dest, "/temp-queue/") {
		return dest
	}
	return "/queue/" + dest
}
