package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

// WebhookPayload represents the data sent to the webhook
type WebhookPayload struct {
	MessageId        string `json:"messageId,omitempty"`
	Sender           string `json:"sender"`
	Content          string `json:"content"`
	ChatJID          string `json:"chatJID"`
	IsFromMe         bool   `json:"isFromMe"`
	QuotedMessageId  string `json:"quotedMessageId,omitempty"`
	QuotedSender     string `json:"quotedSender,omitempty"`
	QuotedContent    string `json:"quotedContent,omitempty"`
}

// SendWebhook sends a message to the webhook endpoint.
//
// messageId carries the WhatsApp message ID so the listener can thread its
// reply on the exact message (rather than re-deriving it from a stale DB
// lookup). When LISTENER_WEBHOOK_TOKEN is set, the request carries a matching
// X-Listener-Token header so the listener can authenticate the caller.
func SendWebhook(messageId, sender, content, chatJID string, isFromMe bool, quotedMessageId, quotedSender, quotedContent string) {
	webhookURL := os.Getenv("WEBHOOK_URL")
	if webhookURL == "" {
		webhookURL = "http://localhost:8769/whatsapp/webhook"
	}

	payload := WebhookPayload{
		MessageId:       messageId,
		Sender:          sender,
		Content:         content,
		ChatJID:         chatJID,
		IsFromMe:        isFromMe,
		QuotedMessageId: quotedMessageId,
		QuotedSender:    quotedSender,
		QuotedContent:   quotedContent,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("Error marshaling webhook payload: %v\n", err)
		return
	}

	req, err := http.NewRequest("POST", webhookURL, bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("Error building webhook request: %v\n", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if token := os.Getenv("LISTENER_WEBHOOK_TOKEN"); token != "" {
		req.Header.Set("X-Listener-Token", token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Error sending webhook: %v\n", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 200 {
		fmt.Printf("✓ Webhook sent for message from %s\n", sender)
	} else {
		fmt.Printf("⚠ Webhook failed with status %d\n", resp.StatusCode)
	}
}

// In main.go, handleMessage forwards webhooks for messages with text content.
// It will forward self-sent messages when the env var FORWARD_SELF=true.
