package helpers

import (
	"strings"
	"unicode"
)

// dottedCapitalI is LATIN CAPITAL LETTER I WITH DOT ABOVE. Go's Unicode tables
// map it to a plain "i", but .NET's ToLowerInvariant leaves it unchanged. Entity
// names are lowercased on both sides of the wire, so the two SDKs would other-
// wise address different entities for the same name.
const dottedCapitalI = '\u0130'

// ToLowerInvariant lowercases s exactly like .NET's String.ToLowerInvariant, so
// entity names and operation names resolve identically across SDKs. It agrees
// with strings.ToLower for every code point except [dottedCapitalI].
func ToLowerInvariant(s string) string {
	if !strings.ContainsRune(s, dottedCapitalI) {
		return strings.ToLower(s)
	}
	var lowered strings.Builder
	lowered.Grow(len(s))
	for _, r := range s {
		if r == dottedCapitalI {
			lowered.WriteRune(r)
			continue
		}
		lowered.WriteRune(unicode.ToLower(r))
	}
	return lowered.String()
}
