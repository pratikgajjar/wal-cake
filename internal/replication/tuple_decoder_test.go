package replication

import (
	"encoding/json"
	"testing"
)

func TestNumericKeepsExactDigits(t *testing.T) {
	tests := map[string]string{
		"9999999999999999.99": `9999999999999999.99`,
		"1250.00":             `1250.00`,
		"-0.000000000000001":  `-0.000000000000001`,
		"NaN":                 `"NaN"`,
		"Infinity":            `"Infinity"`,
		"-Infinity":           `"-Infinity"`,
	}
	for in, want := range tests {
		got, err := json.Marshal((&NumericHandler{}).Handle([]byte(in)))
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if string(got) != want {
			t.Errorf("NUMERIC %s encodes as %s, want %s", in, got, want)
		}
	}
}
