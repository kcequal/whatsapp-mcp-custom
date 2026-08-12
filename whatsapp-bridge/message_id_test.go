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
	"reflect"
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
