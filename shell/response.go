package shell

type Response struct {
	Status string      `json:"status"`
	Count  uint64      `json:"count,omitempty"`
	Bucket string      `json:"bucket,omitempty"`
	Message string      `json:"message,omitempty"`
	Body   interface{} `json:"body,omitempty"`
}
