// Package message defines one-way messages and delivery metadata.
package message

const (
	StatusPending = "pending"
	StatusSent    = "sent"
)

type SendRequest struct {
	PipeID          string `json:"pipe_id"`
	SenderID        string `json:"sender_id"`
	ReceiverID      string `json:"receiver_id"`
	ClientMessageID string `json:"client_message_id"`
	Payload         string `json:"payload"`
	ReplyTo         string `json:"reply_to,omitempty"`
}

type Message struct {
	ID              string `json:"id"`
	PipeID          string `json:"pipe_id"`
	SenderID        string `json:"sender_id"`
	ReceiverID      string `json:"receiver_id"`
	ClientMessageID string `json:"client_message_id"`
	Payload         string `json:"payload"`
	ReplyTo         string `json:"reply_to,omitempty"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
	SentAt          string `json:"sent_at,omitempty"`
	DeliverySeq     int64  `json:"delivery_seq,omitempty"`
}

type Receipt struct {
	ID              string `json:"id"`
	PipeID          string `json:"pipe_id"`
	ClientMessageID string `json:"client_message_id"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
}

type StatusResponse struct {
	ID              string `json:"id"`
	PipeID          string `json:"pipe_id"`
	SenderID        string `json:"sender_id"`
	ReceiverID      string `json:"receiver_id"`
	ClientMessageID string `json:"client_message_id"`
	ReplyTo         string `json:"reply_to,omitempty"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
	SentAt          string `json:"sent_at,omitempty"`
	DeliverySeq     int64  `json:"delivery_seq,omitempty"`
}
