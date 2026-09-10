package helpers

import (
	"strconv"
	"strings"
)

// CompareTaskVersions uses numeric comparison for two to four nonnegative
// integer components, otherwise case-insensitive lexical comparison.
// An empty version sorts before every nonempty version.
func CompareTaskVersions(left, right string) int {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	switch {
	case left == "" && right == "":
		return 0
	case left == "":
		return -1
	case right == "":
		return 1
	}

	leftParts, leftOK := numericVersion(left)
	rightParts, rightOK := numericVersion(right)
	if leftOK && rightOK {
		maxParts := max(len(leftParts), len(rightParts))
		for i := 0; i < maxParts; i++ {
			leftValue := -1
			if i < len(leftParts) {
				leftValue = leftParts[i]
			}
			rightValue := -1
			if i < len(rightParts) {
				rightValue = rightParts[i]
			}
			if leftValue < rightValue {
				return -1
			}
			if leftValue > rightValue {
				return 1
			}
		}
		return 0
	}
	return strings.Compare(strings.ToLower(left), strings.ToLower(right))
}

func numericVersion(version string) ([]int, bool) {
	parts := strings.Split(version, ".")
	if len(parts) < 2 || len(parts) > 4 {
		return nil, false
	}
	values := make([]int, len(parts))
	for i, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return nil, false
		}
		values[i] = value
	}
	return values, true
}
