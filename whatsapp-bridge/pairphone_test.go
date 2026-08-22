package main

import "testing"

// PAIR_PHONE exists because a QR scan cannot be relayed to a remote human:
// the code is valid ~20s and getting it off a headless box and into WhatsApp
// costs 10-15s of that. Measured 2026-08-22 while relinking a production
// bridge -- four consecutive QR attempts expired in flight. A linking code is
// text and stays valid for the whole ~160s login window, so it survives being
// forwarded.
//
// These cases pin the parsing, because the failure mode is silent: an
// unusable PAIR_PHONE that is not rejected at startup shows up 40 seconds
// later as an opaque IQ error, on a box nobody is watching, during exactly
// the outage this feature exists to shorten.
func TestNormalizePairPhone(t *testing.T) {
	cases := []struct {
		name        string
		raw         string
		wantDigits  string
		wantProblem bool
	}{
		{"unset is not an error", "", "", false},
		{"whitespace only is not an error", "   ", "", false},
		{"plain digits pass through", "919499499994", "919499499994", false},
		{"plus and spaces are stripped", "+91 94994 99994", "919499499994", false},
		{"dashes and brackets are stripped", "+1 (555) 123-4567", "15551234567", false},
		{"tabs and newlines around the value are trimmed", "\t919499499994\n", "919499499994", false},

		// Both rejections mirror whatsmeow's own preconditions. If either stops
		// being enforced the bridge still "works" -- it just fails 40s later
		// with a server error instead of at startup.
		{"too short is rejected", "12345", "", true},
		{"exactly six digits is still too short", "123456", "", true},
		{"seven digits is the first accepted length", "1234567", "1234567", false},
		{"leading zero is rejected as national form", "09499499994", "", true},
		{"leading zero survives punctuation stripping and is still rejected", "(0)94-994-99994", "", true},
		{"punctuation only is rejected, not treated as unset", "+++", "", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			digits, problem := normalizePairPhone(tc.raw)
			if digits != tc.wantDigits {
				t.Errorf("normalizePairPhone(%q) digits = %q, want %q", tc.raw, digits, tc.wantDigits)
			}
			if (problem != "") != tc.wantProblem {
				t.Errorf("normalizePairPhone(%q) problem = %q, wantProblem = %v", tc.raw, problem, tc.wantProblem)
			}
			// Never both: the caller switches on one or the other.
			if digits != "" && problem != "" {
				t.Errorf("normalizePairPhone(%q) returned BOTH digits %q and problem %q", tc.raw, digits, problem)
			}
		})
	}
}

// A rejected value must fall back to QR rather than be passed on as empty-ish
// junk. Empty digits is the signal for "do not attempt code pairing", so a
// problem case must never leak a partial number.
func TestRejectedPairPhoneYieldsNoDigits(t *testing.T) {
	for _, raw := range []string{"12345", "09499499994", "+++"} {
		digits, problem := normalizePairPhone(raw)
		if problem == "" {
			t.Fatalf("normalizePairPhone(%q) was accepted; expected rejection", raw)
		}
		if digits != "" {
			t.Errorf("normalizePairPhone(%q) rejected but still returned digits %q, which would be sent to PairPhone", raw, digits)
		}
	}
}
