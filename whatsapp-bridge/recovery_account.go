package main

import (
	"encoding/json"
	"errors"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"net/http"
	"os"
	"regexp"
)

var recoveryPhonePattern = regexp.MustCompile(`^[1-9][0-9]{7,14}$`)

// Empty preserves legacy behavior. Invalid configuration never disables the guard.
func validateRecoveryPhone(expected string) error {
	if expected != "" && !recoveryPhonePattern.MatchString(expected) {
		return errors.New("WA_RECOVERY_EXPECTED_PHONE must contain 8-15 international digits without a leading zero")
	}
	return nil
}
func recoveryAccountError(client *whatsmeow.Client) error {
	expected := os.Getenv("WA_RECOVERY_EXPECTED_PHONE")
	if err := validateRecoveryPhone(expected); err != nil {
		return err
	}
	if expected == "" {
		return nil
	}
	if client == nil {
		return errors.New("recovery account is not linked")
	}
	own := client.Store.GetJID()
	if own.IsEmpty() {
		return errors.New("recovery account is not linked")
	}
	if own.User != expected || own.Server != types.DefaultUserServer {
		return errors.New("recovery account mismatch; account traffic is blocked")
	}
	return nil
}

// Protect the complete HTTP surface: health, media, receipts, edits and sends.
func recoveryAccountHTTP(client *whatsmeow.Client, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := recoveryAccountError(client); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "connected": false, "status": "account_blocked", "account_guard_enabled": true, "account_blocked": true, "message": err.Error()})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Events precede REST startup; block wrong-account history and live messages
// before the application writes its backlog database or forwards webhooks.
func recoveryAccountEvents(client *whatsmeow.Client, next func(interface{})) func(interface{}) {
	return func(event interface{}) {
		if recoveryAccountError(client) != nil {
			// These carry lifecycle information only. Logout must still be logged
			// after whatsmeow clears the stored ID so recovery can detect it.
			switch event.(type) {
			case *events.LoggedOut, *events.Disconnected, *events.ConnectFailure:
			default:
				return
			}
		}
		next(event)
	}
}
