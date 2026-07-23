package shared

import (
	"testing"
	"time"
)

func TestPublishStatusReplacesPendingMessage(t *testing.T) {
	status := make(chan string, 1)
	PublishStatus(status, "old")
	PublishStatus(status, "latest\nformatted")
	select {
	case actual := <-status:
		if actual != "latest\nformatted" {
			t.Fatalf("unexpected status %q", actual)
		}
	case <-time.After(time.Second):
		t.Fatal("status was not published")
	}
}

func TestPublishStatusAllowsNilChannel(t *testing.T) {
	PublishStatus(nil, "ignored")
}
