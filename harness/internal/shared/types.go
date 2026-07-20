// Package shared contains data types shared by harness implementations.
package shared

// TokenCount contains token usage dimensions reported by a coding harness.
type TokenCount struct {
	Input      int `json:"input"`
	Output     int `json:"output"`
	Total      int `json:"total"`
	CacheRead  int `json:"cacheRead"`
	CacheWrite int `json:"cacheWrite"`
}
