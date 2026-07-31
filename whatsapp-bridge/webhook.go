package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// Dedicated client with a hard timeout: SendWebhook runs synchronously inside
// the WhatsApp event handler, so a wedged listener on http.DefaultClient (no
// timeout) would stall ALL message ingestion behind one hung POST.
var webhookClient = &http.Client{Timeout: 5 * time.Second}

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
	// Media messages (voice notes etc.) have empty Content; these fields let
	// the listener see them and decide (e.g. transcribe audio DMs).
	MediaType  string `json:"media_type,omitempty"`
	Filename   string `json:"filename,omitempty"`
	FileLength uint64 `json:"file_length,omitempty"`
	IsPTT      bool   `json:"is_ptt,omitempty"`
}

// SendWebhook sends a message to the webhook endpoint.
//
// messageId carries the WhatsApp message ID so the listener can thread its
// reply on the exact message (rather than re-deriving it from a stale DB
// lookup). When LISTENER_WEBHOOK_TOKEN is set, the request carries a matching
// X-Listener-Token header so the listener can authenticate the caller.
func SendWebhook(messageId, sender, content, chatJID string, isFromMe bool, quotedMessageId, quotedSender, quotedContent, mediaType, filename string, fileLength uint64, isPTT bool) {
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
		MediaType:       mediaType,
		Filename:        filename,
		FileLength:      fileLength,
		IsPTT:           isPTT,
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

	resp, err := webhookClient.Do(req)
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
