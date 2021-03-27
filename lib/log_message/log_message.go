package log_message

type LogMessage struct {
	ServiceName string `json:"service-name"`
	Message     string `json:"message"`
	Payload     string `json:"payload"`
}
