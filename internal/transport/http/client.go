// Package httptransport provides the pipecli HTTP client.
package httptransport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/five-zombie/agent-pipe/internal/message"
	"github.com/five-zombie/agent-pipe/internal/pipe"
)

type Client struct {
	BaseURL  string
	Token    string
	ClientID string
	HTTP     *http.Client
}

func New(baseURL, token, clientID string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, ClientID: clientID, HTTP: http.DefaultClient}
}

func (c *Client) CreatePipe(ctx context.Context, req pipe.CreateRequest) (pipe.Pipe, error) {
	var result pipe.Pipe
	err := c.json(ctx, http.MethodPost, "/v1/pipes", req, &result)
	return result, err
}

func (c *Client) Join(ctx context.Context, pipeID string, req pipe.JoinRequest) (pipe.Pipe, error) {
	var result pipe.Pipe
	err := c.json(ctx, http.MethodPost, "/v1/pipes/"+url.PathEscape(pipeID)+"/join", req, &result)
	return result, err
}

func (c *Client) Send(ctx context.Context, req message.SendRequest) (message.Receipt, error) {
	var result message.Receipt
	err := c.json(ctx, http.MethodPost, "/v1/messages", req, &result)
	return result, err
}

func (c *Client) Status(ctx context.Context, messageID, clientID string) (message.StatusResponse, error) {
	var result message.StatusResponse
	path := "/v1/messages/" + url.PathEscape(messageID) + "/status?client_id=" + url.QueryEscape(clientID)
	err := c.json(ctx, http.MethodGet, path, nil, &result)
	return result, err
}

func (c *Client) Listen(ctx context.Context, pipeID, clientID, lastEventID string) (io.ReadCloser, error) {
	path := "/v1/pipes/" + url.PathEscape(pipeID) + "/events?client_id=" + url.QueryEscape(clientID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	c.authorize(req)
	resp, err := c.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		defer resp.Body.Close()
		return nil, responseError(resp)
	}
	return resp.Body, nil
}

func (c *Client) json(ctx context.Context, method, path string, body any, target any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		reader = strings.NewReader(string(data))
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c.authorize(req)
	resp, err := c.client().Do(req)
	if err != nil {
		return fmt.Errorf("request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return responseError(resp)
	}
	if target == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) client() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func (c *Client) authorize(req *http.Request) {
	if c.ClientID != "" {
		req.Header.Set("X-AIPipe-Client-ID", c.ClientID)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
}

func responseError(resp *http.Response) error {
	var body struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(bufio.NewReader(resp.Body)).Decode(&body); err == nil && body.Error != "" {
		return fmt.Errorf("server returned %s: %s", resp.Status, body.Error)
	}
	return fmt.Errorf("server returned %s", resp.Status)
}
