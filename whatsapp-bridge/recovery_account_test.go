package main

import (
	"encoding/json"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"net/http"
	"net/http/httptest"
	"testing"
)

func accountClient(phone string) *whatsmeow.Client {
	device := &store.Device{}
	if phone != "" {
		jid := types.NewJID(phone, types.DefaultUserServer)
		jid.Device = 8
		device.ID = &jid
	}
	return &whatsmeow.Client{Store: device}
}
func TestRecoveryAccountGuard(t *testing.T) {
	for _, tc := range []struct {
		expected, actual string
		allowed          bool
	}{
		{"", "", true}, {"", "919999999999", true}, {"919949603030", "919949603030", true},
		{"919949603030", "919999999999", false}, {"919949603030", "", false},
		{"+919949603030", "919949603030", false}, {" 919949603030", "919949603030", false},
		{"0919949603030", "0919949603030", false}, {"abc", "abc", false},
	} {
		t.Run(tc.expected+"/"+tc.actual, func(t *testing.T) {
			t.Setenv("WA_RECOVERY_EXPECTED_PHONE", tc.expected)
			if got := recoveryAccountError(accountClient(tc.actual)) == nil; got != tc.allowed {
				t.Fatalf("allowed=%v want %v", got, tc.allowed)
			}
		})
	}
	t.Setenv("WA_RECOVERY_EXPECTED_PHONE", "919949603030")
	for _, client := range []*whatsmeow.Client{nil, {}, accountClient("")} {
		if recoveryAccountError(client) == nil {
			t.Fatal("missing identity accepted")
		}
	}
	client := accountClient("919949603030")
	client.Store.ID.Server = types.HiddenUserServer
	if recoveryAccountError(client) == nil {
		t.Fatal("LID accepted as phone")
	}
}
func TestRecoveryAccountBlocksEveryHTTPRoute(t *testing.T) {
	t.Setenv("WA_RECOVERY_EXPECTED_PHONE", "919949603030")
	for _, actual := range []string{"919999999999", ""} {
		called := 0
		handler := bridgeRESTHandler(accountClient(actual), nil)
		for _, route := range []string{"health", "send", "reply", "forward", "edit", "revoke", "read", "mark_read", "location", "sticker", "react", "typing", "download", "request_history"} {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest("POST", "/api/"+route, nil))
			var body map[string]interface{}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != 503 || body["success"] != false || body["connected"] != false || body["account_guard_enabled"] != true || body["account_blocked"] != true {
				t.Fatalf("%s accepted: %s", route, recorder.Body.String())
			}
		}
		if called != 0 {
			t.Fatal("blocked request reached application")
		}
	}
	called := false
	recoveryAccountHTTP(accountClient("919949603030"), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/health", nil))
	if !called {
		t.Fatal("expected account blocked")
	}
}
func TestRecoveryAccountBlocksEventsAndDirectSend(t *testing.T) {
	t.Setenv("WA_RECOVERY_EXPECTED_PHONE", "919949603030")
	called := 0
	wrong := accountClient("919999999999")
	handler := recoveryAccountEvents(wrong, func(interface{}) { called++ })
	handler("message")
	handler("history")
	handler("call")
	if called != 0 {
		t.Fatal("wrong-account event reached backlog handler")
	}
	accepted, message, id := sendWhatsAppMessage(wrong, nil, "919499499994", "backlog", "")
	if accepted || id != "" || message != "recovery account mismatch; account traffic is blocked" {
		t.Fatal("wrong-account send not blocked by identity guard")
	}
	recoveryAccountEvents(accountClient("919949603030"), func(interface{}) { called++ })("message")
	if called != 1 {
		t.Fatal("expected-account event blocked")
	}
}

func TestRecoveryAccountActualHTTPHandlers(t *testing.T) {
	t.Setenv("WA_RECOVERY_EXPECTED_PHONE", "919949603030")
	for _, phone := range []string{"", "919999999999"} {
		handler := bridgeRESTHandler(accountClient(phone), nil)
		for _, route := range []string{"health", "send", "reply", "forward", "edit", "revoke", "read", "typing", "location", "sticker", "react", "download", "history"} {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest("POST", "/api/"+route, nil))
			if w.Code != 503 {
				t.Fatalf("%s got %d: %s", route, w.Code, w.Body.String())
			}
		}
	}
}

func TestRecoveryAccountHealthTelemetry(t *testing.T) {
	for _, expected := range []string{"", "919949603030"} {
		t.Setenv("WA_RECOVERY_EXPECTED_PHONE", expected)
		w := httptest.NewRecorder()
		bridgeRESTHandler(accountClient("919949603030"), nil).ServeHTTP(w, httptest.NewRequest("GET", "/api/health", nil))
		var body map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body["account_guard_enabled"] != (expected != "") || body["account_blocked"] != false {
			t.Fatalf("wrong telemetry: %s", w.Body.String())
		}
	}
}

func TestRecoveryAccountPreservesLogoutSignal(t *testing.T) {
	t.Setenv("WA_RECOVERY_EXPECTED_PHONE", "919949603030")
	called := 0
	handler := recoveryAccountEvents(accountClient(""), func(interface{}) { called++ })
	handler(&events.LoggedOut{})
	handler(&events.Disconnected{})
	handler(&events.ConnectFailure{})
	if called != 3 {
		t.Fatal("lost lifecycle signals after device ID cleared")
	}
	handler(&events.Message{})
	handler(&events.HistorySync{})
	handler(&events.Connected{})
	if called != 3 {
		t.Fatal("unlinked account traffic or connected signal allowed")
	}
}
