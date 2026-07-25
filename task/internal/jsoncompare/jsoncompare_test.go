package jsoncompare

import "testing"

func TestEqualNormalizesJSON(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		actual   string
		equal    bool
	}{
		{name: "object order and whitespace", expected: `{"status":"accepted","value":1}`, actual: "{\n  \"value\": 1.0,\n  \"status\": \"accepted\"\n}", equal: true},
		{name: "equivalent exponents", expected: `{"value":100}`, actual: `{"value":1e2}`, equal: true},
		{name: "large exact numbers differ", expected: `9007199254740992`, actual: `9007199254740993`, equal: false},
		{name: "array order matters", expected: `[1,2]`, actual: `[2,1]`, equal: false},
		{name: "types differ", expected: `1`, actual: `"1"`, equal: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			equal, err := Equal([]byte(test.expected), []byte(test.actual))
			if err != nil {
				t.Fatal(err)
			}
			if equal != test.equal {
				t.Fatalf("Equal() = %t, want %t", equal, test.equal)
			}
		})
	}
}

func TestEqualResponseUsesSemanticJSONAndExactTextComparison(t *testing.T) {
	tests := []struct {
		expected, actual string
		equal            bool
	}{
		{expected: `{"value":1}`, actual: `{ "value": 1.0 }`, equal: true},
		{expected: "plain text", actual: "plain text", equal: true},
		{expected: "plain text", actual: "plain  text", equal: false},
		{expected: "", actual: "", equal: true},
	}
	for _, test := range tests {
		equal, err := EqualResponse([]byte(test.expected), []byte(test.actual))
		if err != nil {
			t.Fatal(err)
		}
		if equal != test.equal {
			t.Errorf("EqualResponse(%q, %q) = %t, want %t", test.expected, test.actual, equal, test.equal)
		}
	}
}

func TestEqualRejectsInvalidOrMultipleJSONValues(t *testing.T) {
	for _, actual := range []string{`not-json`, `{} {}`} {
		if _, err := Equal([]byte(`{}`), []byte(actual)); err == nil {
			t.Errorf("Equal accepted %q", actual)
		}
	}
}

func TestIsJSONContentType(t *testing.T) {
	for _, value := range []string{"application/json", "application/json; charset=utf-8", "Application/JSON; Charset=UTF-8"} {
		if !IsJSONContentType(value) {
			t.Errorf("IsJSONContentType(%q) = false", value)
		}
	}
	for _, value := range []string{"", "text/json", "application/json garbage"} {
		if IsJSONContentType(value) {
			t.Errorf("IsJSONContentType(%q) = true", value)
		}
	}
}
