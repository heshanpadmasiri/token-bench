// Package jsoncompare provides semantic JSON and media-type comparisons for task validation.
package jsoncompare

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"mime"
	"strings"
)

// Equal reports whether expected and actual contain semantically equivalent JSON.
// Object order, insignificant whitespace, and equivalent JSON number forms are ignored.
func Equal(expected, actual []byte) (bool, error) {
	expectedValue, err := decode(expected)
	if err != nil {
		return false, fmt.Errorf("decode expected JSON: %w", err)
	}
	actualValue, err := decode(actual)
	if err != nil {
		return false, fmt.Errorf("decode actual JSON: %w", err)
	}
	return equalValue(expectedValue, actualValue), nil
}

// EqualResponse compares valid expected JSON semantically and other response
// bodies byte-for-byte. It is useful for contracts that may relay JSON, text,
// or an empty body.
func EqualResponse(expected, actual []byte) (bool, error) {
	if json.Valid(expected) {
		return Equal(expected, actual)
	}
	return bytes.Equal(expected, actual), nil
}

// IsJSONContentType reports whether value has the application/json media type.
// Media-type parameters such as charset are allowed.
func IsJSONContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(mediaType, "application/json")
}

func decode(value []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	return decoded, nil
}

func equalValue(expected, actual any) bool {
	switch expected := expected.(type) {
	case map[string]any:
		actual, ok := actual.(map[string]any)
		if !ok || len(expected) != len(actual) {
			return false
		}
		for key, expectedValue := range expected {
			actualValue, exists := actual[key]
			if !exists || !equalValue(expectedValue, actualValue) {
				return false
			}
		}
		return true
	case []any:
		actual, ok := actual.([]any)
		if !ok || len(expected) != len(actual) {
			return false
		}
		for index := range expected {
			if !equalValue(expected[index], actual[index]) {
				return false
			}
		}
		return true
	case json.Number:
		actual, ok := actual.(json.Number)
		return ok && equalNumber(string(expected), string(actual))
	case string:
		actual, ok := actual.(string)
		return ok && expected == actual
	case bool:
		actual, ok := actual.(bool)
		return ok && expected == actual
	case nil:
		return actual == nil
	default:
		return false
	}
}

type decimal struct {
	negative bool
	digits   string
	exponent *big.Int
}

func equalNumber(left, right string) bool {
	leftDecimal, leftOK := parseDecimal(left)
	rightDecimal, rightOK := parseDecimal(right)
	return leftOK && rightOK && leftDecimal.negative == rightDecimal.negative &&
		leftDecimal.digits == rightDecimal.digits && leftDecimal.exponent.Cmp(rightDecimal.exponent) == 0
}

func parseDecimal(value string) (decimal, bool) {
	result := decimal{exponent: new(big.Int)}
	if strings.HasPrefix(value, "-") {
		result.negative = true
		value = value[1:]
	}
	if index := strings.IndexAny(value, "eE"); index >= 0 {
		if _, ok := result.exponent.SetString(value[index+1:], 10); !ok {
			return decimal{}, false
		}
		value = value[:index]
	}
	fractionLength := 0
	if index := strings.IndexByte(value, '.'); index >= 0 {
		fractionLength = len(value) - index - 1
		value = value[:index] + value[index+1:]
	}
	value = strings.TrimLeft(value, "0")
	if value == "" {
		return decimal{digits: "0", exponent: new(big.Int)}, true
	}
	result.exponent.Sub(result.exponent, big.NewInt(int64(fractionLength)))
	trimmed := strings.TrimRight(value, "0")
	result.exponent.Add(result.exponent, big.NewInt(int64(len(value)-len(trimmed))))
	result.digits = trimmed
	return result, true
}
