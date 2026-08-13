package main

// Step 1 of the one-send-path v3 rollout (LLD §7, LLD §8 test 4).
//
// WHY THESE TESTS EXIST. The ledger on the Buddy side already has a
// message_id column and contact_broker.py already reads "message_id" off the
// bridge's /api/send response — but the bridge never sends one. So every row
// records a NULL id, and "the bridge said success" is the only evidence a send
// ever happened. That is exactly the "accepted != delivered" confusion this
// step closes: without the provider's own id, Buddy cannot distinguish a
// message WhatsApp accepted and gave a handle for from one it merely did not
// error on.
//
// The first two tests are REFLECTION tests on purpose. A test that referred to
// the new field or the new third return value directly would fail to COMPILE
// against today's code, and a build failure is a weak red — it proves the
// symbol is missing, not that the contract is wrong. Reflection compiles
// against the unchanged bridge and fails with a real assertion, so the red is
// about behaviour.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
)

// The provider id must reach the caller of sendWhatsAppMessage. Today it is
// discarded: whatsmeow hands us resp.ID at the client.SendMessage call and the
// function throws it away, returning only (ok, humanMessage).
func TestSendWhatsAppMessage_ReturnsProviderMessageID(t *testing.T) {
	fn := reflect.TypeOf(sendWhatsAppMessage)

	if got := fn.NumOut(); got != 3 {
		t.Fatalf("sendWhatsAppMessage returns %d values, want 3 (ok, message, messageID): "+
			"the provider message id is being discarded, so the ledger records NULL", got)
	}
	if got := fn.Out(2).Kind(); got != reflect.String {
		t.Fatalf("third return value is %s, want string (the provider message id)", got)
	}
}

// The HTTP response must carry the id under the exact key contact_broker.py
// reads ("message_id"), and must OMIT it when the provider gave none, so an
// absent id is distinguishable from an empty one. Buddy treats "accepted with
// no id" differently from "accepted with an id" — a present-but-empty key
// would collapse that distinction.
func TestSendMessageResponse_CarriesMessageIDField(t *testing.T) {
	rt := reflect.TypeOf(SendMessageResponse{})

	field, ok := rt.FieldByName("MessageID")
	if !ok {
		t.Fatalf("SendMessageResponse has no MessageID field; fields present: %s", fieldNames(rt))
	}
	if field.Type.Kind() != reflect.String {
		t.Fatalf("MessageID is %s, want string", field.Type.Kind())
	}
	if got := field.Tag.Get("json"); got != "message_id,omitempty" {
		t.Fatalf("MessageID json tag is %q, want %q — contact_broker.py reads data[\"message_id\"], "+
			"and omitempty keeps 'accepted without an id' distinguishable from an empty id",
			got, "message_id,omitempty")
	}
}

func fieldNames(rt reflect.Type) []string {
	names := make([]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		names = append(names, rt.Field(i).Name)
	}
	return names
}

// ---------------------------------------------------------------------------
// Wire-level tests. These go through the real /api/send handler with a fake
// sender, so they assert the JSON bytes contact_broker.py actually parses,
// not just the Go types.
// ---------------------------------------------------------------------------

func postSend(t *testing.T, send sendFunc, body string) (int, map[string]any, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/send", strings.NewReader(body))
	sendHandler(send)(rec, req)

	raw := rec.Body.String()
	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("response is not JSON: %v (body %q)", err, raw)
	}
	return rec.Code, decoded, raw
}

// The id WhatsApp gave us must arrive at the caller, unaltered, under
// "message_id" — the key contact_broker.py already reads.
func TestSendHandler_EmitsProviderMessageID(t *testing.T) {
	const providerID = "3EB0C767D26B8FA1B2C3"

	code, got, raw := postSend(t, func(recipient, message, mediaPath string) (bool, string, string) {
		return true, "Message sent to 919999999999", providerID
	}, `{"recipient":"919999999999","message":"hello"}`)

	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", code, raw)
	}
	if got["message_id"] != providerID {
		t.Fatalf("message_id = %v, want %q — the ledger records what this key holds (body %q)",
			got["message_id"], providerID, raw)
	}
	if got["success"] != true {
		t.Fatalf("success = %v, want true (body %q)", got["success"], raw)
	}
}

// LLD 8 test 4 — ACCEPTED IS NOT DELIVERED. The bridge can report success
// without an id (WhatsApp returned no handle). The caller must be able to see
// that: the key is ABSENT, not present-and-empty, so "handed over" cannot be
// mistaken for "delivered with a handle".
func TestSendHandler_AcceptedWithoutIDOmitsTheKey(t *testing.T) {
	code, got, raw := postSend(t, func(recipient, message, mediaPath string) (bool, string, string) {
		return true, "Message sent to 919999999999", ""
	}, `{"recipient":"919999999999","message":"hello"}`)

	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", code, raw)
	}
	if got["success"] != true {
		t.Fatalf("success = %v, want true (body %q)", got["success"], raw)
	}
	if _, present := got["message_id"]; present {
		t.Fatalf("message_id key is present as %#v; accepted-without-an-id must OMIT it so the "+
			"caller can tell 'handed over' from 'handed over with a provider handle' (body %q)",
			got["message_id"], raw)
	}
}

// A failed send carries no id at all, and still reports 500. An id on a
// failure path would be a fabricated handle for a message that never went.
func TestSendHandler_FailureCarriesNoMessageID(t *testing.T) {
	code, got, raw := postSend(t, func(recipient, message, mediaPath string) (bool, string, string) {
		return false, "Error sending message: websocket not connected", ""
	}, `{"recipient":"919999999999","message":"hello"}`)

	if code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body %q)", code, raw)
	}
	if got["success"] != false {
		t.Fatalf("success = %v, want false (body %q)", got["success"], raw)
	}
	if _, present := got["message_id"]; present {
		t.Fatalf("failed send emitted message_id %#v (body %q)", got["message_id"], raw)
	}
}

// The extraction must not have changed the guard rails on the way in: a send
// is never attempted for a bad method, unparseable body, missing recipient, or
// a request with neither text nor media.
func TestSendHandler_RejectsBadRequestsWithoutSending(t *testing.T) {
	cases := []struct {
		name   string
		method string
		body   string
		want   int
	}{
		{"GET is refused", http.MethodGet, ``, http.StatusMethodNotAllowed},
		{"malformed JSON is refused", http.MethodPost, `{`, http.StatusBadRequest},
		{"no recipient is refused", http.MethodPost, `{"message":"hi"}`, http.StatusBadRequest},
		{"no body and no media is refused", http.MethodPost, `{"recipient":"919999999999"}`, http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			send := func(recipient, message, mediaPath string) (bool, string, string) {
				called = true
				return true, "sent", "SHOULD-NOT-HAPPEN"
			}
			rec := httptest.NewRecorder()
			sendHandler(send)(rec, httptest.NewRequest(tc.method, "/api/send", strings.NewReader(tc.body)))

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
			}
			if called {
				t.Fatalf("a WhatsApp send was attempted for a request that should have been refused")
			}
		})
	}
}

// Media-only sends are legal (LLD 1: "body may be NULL, attachment-only sends
// are legal") and must still return the provider id.
func TestSendHandler_MediaOnlySendStillCarriesTheID(t *testing.T) {
	const providerID = "3EB0MEDIA0001"

	var gotMediaPath string
	code, got, raw := postSend(t, func(recipient, message, mediaPath string) (bool, string, string) {
		gotMediaPath = mediaPath
		return true, "Message sent to 919999999999", providerID
	}, `{"recipient":"919999999999","media_path":"/tmp/spool/report.pdf"}`)

	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", code, raw)
	}
	if gotMediaPath != "/tmp/spool/report.pdf" {
		t.Fatalf("media_path reached the sender as %q", gotMediaPath)
	}
	if got["message_id"] != providerID {
		t.Fatalf("message_id = %v, want %q (body %q)", got["message_id"], providerID, raw)
	}
}

// NO FABRICATED HANDLES. Every failure path out of sendWhatsAppMessage must
// return an empty message id. An id attached to a send that did not happen is
// a handle for a message that does not exist — the ledger would then hold
// evidence of a delivery nobody made, which is the exact failure this whole
// project exists to kill.
//
// This is a SOURCE-level test because the failure paths need a live whatsmeow
// client to reach at runtime. It is not a substitute for a behavioural test;
// it is the strongest guard available for a path that cannot be driven here,
// and it does catch the mutation "return an id on failure".
func TestSendWhatsAppMessage_NoFailurePathReturnsAnID(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("reading main.go: %v", err)
	}
	lines := strings.Split(string(src), "\n")

	start := -1
	for i, line := range lines {
		if strings.HasPrefix(line, "func sendWhatsAppMessage(") {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatal("could not find sendWhatsAppMessage in main.go")
	}
	end := -1
	for i := start + 1; i < len(lines); i++ {
		if lines[i] == "}" {
			end = i
			break
		}
	}
	if end < 0 {
		t.Fatal("could not find the end of sendWhatsAppMessage")
	}

	failures := 0
	for i := start; i < end; i++ {
		trimmed := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(trimmed, "return false,") {
			continue
		}
		failures++
		if !strings.HasSuffix(trimmed, `, ""`) {
			t.Errorf("main.go:%d returns a message id on a FAILURE path: %s", i+1, trimmed)
		}
	}
	if failures == 0 {
		t.Fatal("found no failure returns in sendWhatsAppMessage — this test has stopped testing anything")
	}
	t.Logf("checked %d failure return paths, all carry an empty message id", failures)
}
