package helpers

import (
	"strings"
	"testing"
)

// Entity names are lowercased on both sides of the wire, so Go and .NET must
// agree for every code point or the two SDKs address different entities. The
// expectations below were produced by running String.ToLowerInvariant on .NET.
func TestToLowerInvariantMatchesDotNet(t *testing.T) {
	tests := map[string]string{
		"Counter":  "counter",
		"COUNTER":  "counter",
		"counter":  "counter",
		"":         "",
		"Ünïcode":  "ünïcode",
		"STRASSE":  "strasse",
		"ΣIGMA":    "σigma",
		"Ⅻ":        "ⅻ",
		"ĸ":        "ĸ",
		"ǅ":        "ǆ",
		"K":        "k", // U+212A KELVIN SIGN
		"İstanbul": "İstanbul",
		"İ":        "İ", // U+0130 is unchanged by ToLowerInvariant
		"ı":        "ı",
		"AİB":      "aİb",
	}
	for input, want := range tests {
		if got := ToLowerInvariant(input); got != want {
			t.Errorf("ToLowerInvariant(%q) = %q, want %q", input, got, want)
		}
	}
}

// U+0130 is the only code point where Go's Unicode tables disagree with .NET.
func TestToLowerInvariantOnlyDivergesForDottedCapitalI(t *testing.T) {
	for cp := rune(0); cp <= 0x10FFFF; cp++ {
		if cp >= 0xD800 && cp <= 0xDFFF {
			continue
		}
		input := string(cp)
		got := ToLowerInvariant(input)
		want := strings.ToLower(input)
		if cp == dottedCapitalI {
			if got != input {
				t.Fatalf("ToLowerInvariant(U+0130) = %q, want it unchanged", got)
			}
			continue
		}
		if got != want {
			t.Fatalf("ToLowerInvariant(%U) = %q, want %q", cp, got, want)
		}
	}
}
