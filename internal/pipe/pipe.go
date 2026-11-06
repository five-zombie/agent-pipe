// Package pipe defines pipe identity and membership operations.
package pipe

type CreateRequest struct {
	ID       string `json:"id,omitempty"`
	ClientID string `json:"client_id"`
	Name     string `json:"name"`
	Desc     string `json:"decs"`
}

type JoinRequest struct {
	ClientID string `json:"client_id"`
	Name     string `json:"name"`
}

type Pipe struct {
	ID        string `json:"id"`
	Desc      string `json:"decs"`
	CreatedAt string `json:"created_at"`
}

type Membership struct {
	PipeID   string `json:"pipe_id"`
	ClientID string `json:"client_id"`
	Name     string `json:"name"`
}
