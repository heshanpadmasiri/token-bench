package shared

// PublishStatus publishes message without allowing status rendering to block a
// harness protocol reader. A pending stale message is replaced by the latest
// one. The channel must have capacity one and remains owned by the caller.
func PublishStatus(status chan string, message string) {
	if status == nil {
		return
	}
	select {
	case status <- message:
		return
	default:
	}
	select {
	case <-status:
	default:
	}
	select {
	case status <- message:
	default:
	}
}
