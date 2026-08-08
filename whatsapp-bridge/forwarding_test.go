package main

import (
	"encoding/json"
	"testing"

	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// The table the rollout depends on. Every row states what the bridge SAW and
// what it must therefore report — including the rows where it saw nothing,
// because "could not inspect" is the answer that keeps the consumer honest.
func TestInspectForwarding(t *testing.T) {
	txt := func(ci *waProto.ContextInfo) *waProto.Message {
		return &waProto.Message{ExtendedTextMessage: &waProto.ExtendedTextMessage{
			Text: proto.String("hi"), ContextInfo: ci}}
	}

	cases := []struct {
		name       string
		msg        *waProto.Message
		wantStatus ForwardingInspectionStatus
		wantFwd    *bool
		wantScore  *uint32
	}{
		{"nil message", nil, "", nil, nil},
		{
			"plain text cannot carry ContextInfo, so it is inspected and clean",
			&waProto.Message{Conversation: proto.String("shoot Sai a note")},
			ForwardingInspected, proto.Bool(false), proto.Uint32(0),
		},
		{
			"extended text with no ContextInfo",
			txt(nil), ForwardingInspected, proto.Bool(false), proto.Uint32(0),
		},
		{
			"ContextInfo present but forwarding fields absent",
			txt(&waProto.ContextInfo{}),
			ForwardingInspected, proto.Bool(false), proto.Uint32(0),
		},
		{
			"explicitly not forwarded",
			txt(&waProto.ContextInfo{IsForwarded: proto.Bool(false),
				ForwardingScore: proto.Uint32(0)}),
			ForwardingInspected, proto.Bool(false), proto.Uint32(0),
		},
		{
			"forwarded",
			txt(&waProto.ContextInfo{IsForwarded: proto.Bool(true),
				ForwardingScore: proto.Uint32(1)}),
			ForwardingInspected, proto.Bool(true), proto.Uint32(1),
		},
		{
			"a score without the flag is still forwarding evidence",
			txt(&waProto.ContextInfo{ForwardingScore: proto.Uint32(5)}),
			ForwardingInspected, proto.Bool(false), proto.Uint32(5),
		},
		{
			"a quote is not a forward — they are separate facts",
			txt(&waProto.ContextInfo{StanzaID: proto.String("Q1")}),
			ForwardingInspected, proto.Bool(false), proto.Uint32(0),
		},
		{
			"image carries its own ContextInfo",
			&waProto.Message{ImageMessage: &waProto.ImageMessage{
				ContextInfo: &waProto.ContextInfo{IsForwarded: proto.Bool(true)}}},
			ForwardingInspected, proto.Bool(true), proto.Uint32(0),
		},
		{
			"a carrier we do not read must SAY so, not default to clean",
			&waProto.Message{StickerMessage: &waProto.StickerMessage{}},
			ForwardingUnsupportedCarrier, nil, nil,
		},
		{
			"an empty message is not a carrier we read",
			&waProto.Message{}, ForwardingUnsupportedCarrier, nil, nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := inspectForwarding(c.msg)
			if c.msg == nil {
				if got != nil {
					t.Fatalf("nil message must inspect to nil, got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("non-nil message must always produce an inspection")
			}
			if got.InspectionStatus != c.wantStatus {
				t.Errorf("status = %q, want %q", got.InspectionStatus, c.wantStatus)
			}
			if (got.IsForwarded == nil) != (c.wantFwd == nil) {
				t.Fatalf("is_forwarded presence = %v, want %v",
					got.IsForwarded != nil, c.wantFwd != nil)
			}
			if c.wantFwd != nil && *got.IsForwarded != *c.wantFwd {
				t.Errorf("is_forwarded = %v, want %v", *got.IsForwarded, *c.wantFwd)
			}
			if (got.ForwardingScore == nil) != (c.wantScore == nil) {
				t.Fatalf("score presence = %v, want %v",
					got.ForwardingScore != nil, c.wantScore != nil)
			}
			if c.wantScore != nil && *got.ForwardingScore != *c.wantScore {
				t.Errorf("score = %d, want %d", *got.ForwardingScore, *c.wantScore)
			}
		})
	}
}

// The marshalling trap, asserted rather than trusted: with plain bools and ints
// `omitempty` deletes false and 0, which would make "inspected, not forwarded"
// byte-identical to "never inspected". The consumer refuses on the latter, so
// that collapse would silently authorise sends from forwarded text.
func TestInspectedCleanIsDistinguishableFromNeverInspected(t *testing.T) {
	clean, err := json.Marshal(inspectForwarding(
		&waProto.Message{Conversation: proto.String("hi")}))
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(clean, &back); err != nil {
		t.Fatal(err)
	}
	if _, ok := back["is_forwarded"]; !ok {
		t.Fatalf("explicit false was erased by omitempty: %s", clean)
	}
	if _, ok := back["forwarding_score"]; !ok {
		t.Fatalf("explicit zero score was erased by omitempty: %s", clean)
	}
	if back["inspection_status"] != string(ForwardingInspected) {
		t.Fatalf("status missing from the wire: %s", clean)
	}

	// And an old bridge, which sends no object at all.
	payload, err := json.Marshal(WebhookPayload{MessageId: "A", Sender: "s"})
	if err != nil {
		t.Fatal(err)
	}
	var old map[string]any
	if err := json.Unmarshal(payload, &old); err != nil {
		t.Fatal(err)
	}
	if _, ok := old["forwarding"]; ok {
		t.Fatalf("a payload with no inspection must omit the object: %s", payload)
	}
}
