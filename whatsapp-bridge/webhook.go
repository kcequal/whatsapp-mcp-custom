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

// ForwardingInspectionStatus says whether this bridge was ABLE to look at the
// message's forwarding metadata — which is a different question from what it
// found. The consumer refuses to send from a message it cannot vouch for, so
// "I could not look" must never be expressible as "I looked and it was clean".
type ForwardingInspectionStatus string

const (
	// ForwardingInspected: this carrier is one we know how to read, and the
	// evidence fields below reflect what WhatsApp actually supplied.
	ForwardingInspected ForwardingInspectionStatus = "inspected"
	// ForwardingUnsupportedCarrier: a message shape this bridge cannot read
	// forwarding metadata from. Reported honestly rather than defaulted.
	ForwardingUnsupportedCarrier ForwardingInspectionStatus = "unsupported_carrier"
)

// ForwardingInspection is EVIDENCE plus the status of the inspection, never a
// verdict. Deciding what it means belongs to the brain, not to a transport
// adapter.
//
// Both evidence fields are POINTERS on purpose, and this is the subtle part:
// Go's `omitempty` erases a `false` bool and a `0` int, so plain scalars would
// make "explicitly not forwarded" byte-identical to "never checked". A pointer
// distinguishes absent from present-and-zero. The OUTER pointer on the payload
// field does the same job one level up: no object at all means an OLD BRIDGE
// that knew nothing about forwarding, which the consumer must also treat as
// unproven.
type ForwardingInspection struct {
	InspectionStatus ForwardingInspectionStatus `json:"inspection_status"`
	IsForwarded      *bool                      `json:"is_forwarded,omitempty"`
	ForwardingScore  *uint32                    `json:"forwarding_score,omitempty"`
}

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

	// Nil means this bridge never inspected forwarding at all (an old build).
	// The consumer fails closed on that, so adding the field is additive: an
	// old listener ignores it, and a new listener refuses rather than assumes.
	Forwarding *ForwardingInspection `json:"forwarding,omitempty"`
}

// SendWebhook sends a message to the webhook endpoint.
//
// messageId carries the WhatsApp message ID so the listener can thread its
// reply on the exact message (rather than re-deriving it from a stale DB
// lookup). When LISTENER_WEBHOOK_TOKEN is set, the request carries a matching
// X-Listener-Token header so the listener can authenticate the caller.
func SendWebhook(messageId, sender, content, chatJID string, isFromMe bool, quotedMessageId, quotedSender, quotedContent, mediaType, filename string, fileLength uint64, isPTT bool, forwarding *ForwardingInspection) {
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
		Forwarding:      forwarding,
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
