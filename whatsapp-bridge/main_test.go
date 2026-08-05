package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// --- Test helpers ---

// mockLIDStore implements store.LIDStore with a simple in-memory map.
type mockLIDStore struct {
	store.NoopStore
	pnByLID map[types.JID]types.JID
}

func (m *mockLIDStore) GetPNForLID(_ context.Context, lid types.JID) (types.JID, error) {
	if pn, ok := m.pnByLID[lid]; ok {
		return pn, nil
	}
	return types.EmptyJID, nil
}

func newTestClient(lidStore store.LIDStore) *whatsmeow.Client {
	noop := &store.NoopStore{}
	return &whatsmeow.Client{
		Store: &store.Device{
			LIDs:     lidStore,
			Contacts: noop,
		},
	}
}

func newTestMessageStore(t *testing.T) *MessageStore {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		);
		CREATE TABLE messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			sender_lid TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BLOB,
			file_sha256 BLOB,
			file_enc_sha256 BLOB,
			file_length INTEGER,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);
	
		CREATE TABLE bridge_meta (key TEXT PRIMARY KEY, value TEXT);

		CREATE VIRTUAL TABLE messages_fts USING fts5(
			content, sender, chat_jid,
			content='messages',
			content_rowid='rowid',
			tokenize='unicode61 remove_diacritics 2'
		);
	`)
	if err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &MessageStore{db: db, ftsEnabled: true}
}

func testLogger() waLog.Logger {
	return waLog.Stdout("Test", "WARN", true)
}

// buildTextMessage constructs an events.Message with the given source fields.
func buildTextMessage(chat, sender, senderAlt, recipientAlt types.JID, isFromMe bool, text string) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:         chat,
				Sender:       sender,
				SenderAlt:    senderAlt,
				RecipientAlt: recipientAlt,
				IsFromMe:     isFromMe,
				IsGroup:      false,
			},
			ID:        "test-msg-001",
			Timestamp: time.Now(),
		},
		Message: &waProto.Message{
			Conversation: proto.String(text),
		},
	}
}

// queryChat returns the chat JID and name, or empty strings if not found.
func queryChat(ms *MessageStore, jid string) (name string, found bool) {
	err := ms.db.QueryRow("SELECT name FROM chats WHERE jid = ?", jid).Scan(&name)
	return name, err == nil
}

// queryChatLastMessageTime returns the last_message_time for a chat JID.
func queryChatLastMessageTime(ms *MessageStore, jid string) (lastMessageTime string, found bool) {
	err := ms.db.QueryRow("SELECT last_message_time FROM chats WHERE jid = ?", jid).Scan(&lastMessageTime)
	return lastMessageTime, err == nil
}

// queryMessageCount returns the number of messages stored under a chat JID.
func queryMessageCount(ms *MessageStore, chatJID string) int {
	var count int
	_ = ms.db.QueryRow("SELECT COUNT(*) FROM messages WHERE chat_jid = ?", chatJID).Scan(&count)
	return count
}

// --- Test fixtures ---

var (
	phoneLID = types.JID{User: "185366493536339", Server: types.HiddenUserServer}
	phonePN  = types.JID{User: "11234567890", Server: types.DefaultUserServer}
)

// --- Integration tests: handleMessage stores under correct JID ---

func TestHandleMessage_IncomingLIDMessage_StoredUnderPhoneJID(t *testing.T) {
	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	logger := testLogger()

	msg := buildTextMessage(
		phoneLID,       // chat: arrives as LID
		phoneLID,       // sender: LID
		phonePN,        // senderAlt: phone JID (provided by whatsmeow)
		types.EmptyJID, // recipientAlt: not set for incoming
		false,          // isFromMe: incoming
		"Hola, qué tal?",
	)

	handleMessage(client, ms, msg, logger)

	// Message MUST be stored under the phone-based JID.
	if count := queryMessageCount(ms, phonePN.String()); count != 1 {
		t.Errorf("expected 1 message under phone JID %s, got %d", phonePN, count)
	}

	// No chat entry should exist for the LID JID.
	if _, found := queryChat(ms, phoneLID.String()); found {
		t.Error("LID chat entry should not exist in database")
	}

	// No message should be stored under the LID JID.
	if count := queryMessageCount(ms, phoneLID.String()); count != 0 {
		t.Errorf("expected 0 messages under LID JID %s, got %d", phoneLID, count)
	}
}

func TestHandleMessage_OutgoingLIDMessage_StoredUnderPhoneJID(t *testing.T) {
	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	logger := testLogger()

	msg := buildTextMessage(
		phoneLID,       // chat: LID
		phoneLID,       // sender: self (LID)
		types.EmptyJID, // senderAlt: not set for outgoing
		phonePN,        // recipientAlt: phone JID
		true,           // isFromMe: outgoing
		"Todo bien!",
	)

	handleMessage(client, ms, msg, logger)

	if count := queryMessageCount(ms, phonePN.String()); count != 1 {
		t.Errorf("expected 1 message under phone JID %s, got %d", phonePN, count)
	}

	if count := queryMessageCount(ms, phoneLID.String()); count != 0 {
		t.Errorf("expected 0 messages under LID JID %s, got %d", phoneLID, count)
	}
}

func TestHandleMessage_LIDWithStoreFallback_StoredUnderPhoneJID(t *testing.T) {
	lidStore := &mockLIDStore{
		pnByLID: map[types.JID]types.JID{phoneLID: phonePN},
	}
	client := newTestClient(lidStore)
	ms := newTestMessageStore(t)
	logger := testLogger()

	// No SenderAlt/RecipientAlt -- must resolve via LID store.
	msg := buildTextMessage(
		phoneLID,       // chat: LID
		phoneLID,       // sender: LID
		types.EmptyJID, // senderAlt: empty (simulates missing alt)
		types.EmptyJID, // recipientAlt: empty
		false,          // isFromMe: incoming
		"Message without alt JIDs",
	)

	handleMessage(client, ms, msg, logger)

	if count := queryMessageCount(ms, phonePN.String()); count != 1 {
		t.Errorf("expected 1 message under phone JID %s, got %d", phonePN, count)
	}

	if count := queryMessageCount(ms, phoneLID.String()); count != 0 {
		t.Errorf("expected 0 messages under LID JID %s, got %d", phoneLID, count)
	}
}

func TestHandleMessage_PhoneJID_Unaffected(t *testing.T) {
	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	logger := testLogger()

	msg := buildTextMessage(
		phonePN,        // chat: already phone-based
		phonePN,        // sender: phone-based
		types.EmptyJID, // senderAlt: empty
		types.EmptyJID, // recipientAlt: empty
		false,          // isFromMe: incoming
		"Normal message",
	)

	handleMessage(client, ms, msg, logger)

	if count := queryMessageCount(ms, phonePN.String()); count != 1 {
		t.Errorf("expected 1 message under phone JID %s, got %d", phonePN, count)
	}
}

func TestMigrateLegacyLIDChatsToPhoneJIDs_MigratesAndIsIdempotent(t *testing.T) {
	ms := newTestMessageStore(t)
	logger := testLogger()

	tmpDir := t.TempDir()
	whatsappDBPath := filepath.Join(tmpDir, "whatsapp.db")

	waDB, err := sql.Open("sqlite3", whatsappDBPath)
	if err != nil {
		t.Fatalf("failed to create whatsapp db: %v", err)
	}
	defer func() { _ = waDB.Close() }()

	if _, err := waDB.Exec(`
		CREATE TABLE whatsmeow_lid_map (
			lid TEXT PRIMARY KEY,
			pn TEXT NOT NULL
		);
		INSERT INTO whatsmeow_lid_map (lid, pn) VALUES ('111', '222');
	`); err != nil {
		t.Fatalf("failed to prepare lid map db: %v", err)
	}

	lidJID := "111@lid"
	phoneJID := "222@s.whatsapp.net"

	_, err = ms.db.Exec(`
		INSERT INTO chats (jid, name, last_message_time) VALUES
			(?, 'Legacy LID Name', '2026-03-01T10:00:00Z'),
			(?, '', '2026-03-01T09:00:00Z');

		INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length) VALUES
			('dup', ?, 'alice', 'lid duplicate', '2026-03-01T10:00:00Z', 0, '', '', '', NULL, NULL, NULL, 0),
			('only-lid', ?, 'alice', 'lid only', '2026-03-01T10:01:00Z', 0, '', '', '', NULL, NULL, NULL, 0),
			('dup', ?, 'alice', 'phone duplicate', '2026-03-01T10:00:00Z', 0, '', '', '', NULL, NULL, NULL, 0),
			('only-phone', ?, 'alice', 'phone only', '2026-03-01T10:02:00Z', 0, '', '', '', NULL, NULL, NULL, 0);
	`, lidJID, phoneJID, lidJID, lidJID, phoneJID, phoneJID)
	if err != nil {
		t.Fatalf("failed to seed message store: %v", err)
	}

	if err := ms.MigrateLegacyLIDChatsToPhoneJIDs(whatsappDBPath, logger); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	if lidCount := queryMessageCount(ms, lidJID); lidCount != 0 {
		t.Fatalf("expected 0 messages under migrated LID chat, got %d", lidCount)
	}
	if phoneCount := queryMessageCount(ms, phoneJID); phoneCount != 3 {
		t.Fatalf("expected 3 messages under phone chat after dedupe, got %d", phoneCount)
	}

	if _, found := queryChat(ms, lidJID); found {
		t.Fatalf("expected migrated LID chat row to be removed")
	}

	phoneName, found := queryChat(ms, phoneJID)
	if !found {
		t.Fatalf("expected phone chat row to exist after migration")
	}
	if phoneName != "Legacy LID Name" {
		t.Fatalf("expected phone chat name to be hydrated from LID chat, got %q", phoneName)
	}

	phoneTime, timeFound := queryChatLastMessageTime(ms, phoneJID)
	if !timeFound {
		t.Fatalf("expected phone chat to have last_message_time after migration")
	}
	if phoneTime != "2026-03-01T10:00:00Z" {
		t.Fatalf("expected phone chat last_message_time to be the latest (from LID chat), got %q", phoneTime)
	}

	if err := ms.MigrateLegacyLIDChatsToPhoneJIDs(whatsappDBPath, logger); err != nil {
		t.Fatalf("second migration run should be a no-op, got error: %v", err)
	}
	if phoneCount := queryMessageCount(ms, phoneJID); phoneCount != 3 {
		t.Fatalf("expected idempotent result with 3 phone messages, got %d", phoneCount)
	}
}

func TestMigrateLegacyLIDChatsToPhoneJIDs_MissingWhatsAppDBIsNoOp(t *testing.T) {
	ms := newTestMessageStore(t)
	logger := testLogger()

	missingPath := filepath.Join(t.TempDir(), "missing-whatsapp.db")
	if err := ms.MigrateLegacyLIDChatsToPhoneJIDs(missingPath, logger); err != nil {
		t.Fatalf("expected missing whatsapp db to be treated as no-op, got error: %v", err)
	}
}

func TestMigrateLegacyLIDChatsToPhoneJIDs_AggregatesByPhoneJIDDeterministically(t *testing.T) {
	ms := newTestMessageStore(t)
	logger := testLogger()

	tmpDir := t.TempDir()
	whatsappDBPath := filepath.Join(tmpDir, "whatsapp.db")

	waDB, err := sql.Open("sqlite3", whatsappDBPath)
	if err != nil {
		t.Fatalf("failed to create whatsapp db: %v", err)
	}
	defer func() { _ = waDB.Close() }()

	if _, err := waDB.Exec(`
		CREATE TABLE whatsmeow_lid_map (
			lid TEXT PRIMARY KEY,
			pn TEXT NOT NULL
		);
		INSERT INTO whatsmeow_lid_map (lid, pn) VALUES ('111', '222');
		INSERT INTO whatsmeow_lid_map (lid, pn) VALUES ('333', '222');
	`); err != nil {
		t.Fatalf("failed to prepare lid map db: %v", err)
	}

	lidA := "111@lid"
	lidB := "333@lid"
	phoneJID := "222@s.whatsapp.net"

	_, err = ms.db.Exec(`
		INSERT INTO chats (jid, name, last_message_time) VALUES
			(?, 'Older Name', '2026-03-01T10:00:00Z'),
			(?, 'Newest Name', '2026-03-01T11:00:00Z');

		INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length) VALUES
			('a1', ?, 'alice', 'from lid A', '2026-03-01T10:00:00Z', 0, '', '', '', NULL, NULL, NULL, 0),
			('b1', ?, 'bob', 'from lid B', '2026-03-01T11:00:00Z', 0, '', '', '', NULL, NULL, NULL, 0);
	`, lidA, lidB, lidA, lidB)
	if err != nil {
		t.Fatalf("failed to seed message store: %v", err)
	}

	if err := ms.MigrateLegacyLIDChatsToPhoneJIDs(whatsappDBPath, logger); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	if count := queryMessageCount(ms, lidA); count != 0 {
		t.Fatalf("expected no messages under first LID after migration, got %d", count)
	}
	if count := queryMessageCount(ms, lidB); count != 0 {
		t.Fatalf("expected no messages under second LID after migration, got %d", count)
	}
	if count := queryMessageCount(ms, phoneJID); count != 2 {
		t.Fatalf("expected 2 messages under phone JID after migration, got %d", count)
	}

	name, found := queryChat(ms, phoneJID)
	if !found {
		t.Fatalf("expected merged phone chat row to exist")
	}
	if name != "Newest Name" {
		t.Fatalf("expected deterministic name selection from latest source chat, got %q", name)
	}

	var lastMessage string
	if err := ms.db.QueryRow("SELECT last_message_time FROM chats WHERE jid = ?", phoneJID).Scan(&lastMessage); err != nil {
		t.Fatalf("failed to read merged last_message_time: %v", err)
	}
	if lastMessage != "2026-03-01T11:00:00Z" {
		t.Fatalf("expected merged last_message_time to be max source value, got %s", lastMessage)
	}
}

// --- Regression tests for the 2026-08-06 QA findings ---

// normalizeSender must not treat a hosted-LID JID as a phone number, and must
// never strip a non-person namespace down to a bare user ("status@broadcast"
// becoming the sender "status", which could collide with a real identity).
func TestNormalizeSender_JIDShapes(t *testing.T) {
	lid := types.JID{User: "185366493536339", Server: types.HiddenUserServer}
	pn := types.JID{User: "11234567890", Server: types.DefaultUserServer}
	client := newTestClient(&mockLIDStore{pnByLID: map[types.JID]types.JID{lid: pn}})

	cases := []struct {
		name       string
		in         types.JID
		wantSender string
		wantLID    string
	}{
		{"resolvable LID", lid, "11234567890", "185366493536339"},
		{"LID with device suffix", types.JID{User: "185366493536339", Device: 8, Server: types.HiddenUserServer}, "11234567890", "185366493536339"},
		{"hosted LID is a LID, not a phone", types.JID{User: "185366493536339", Device: 8, Server: types.HostedLIDServer}, "11234567890", "185366493536339"},
		{"unresolvable LID keeps the @lid marker", types.JID{User: "999888777666555", Server: types.HiddenUserServer}, "999888777666555@lid", "999888777666555"},
		{"phone passes through", pn, "11234567890", ""},
		{"phone with device suffix", types.JID{User: "11234567890", Device: 3, Server: types.DefaultUserServer}, "11234567890", ""},
		{"group JID keeps its namespace", types.JID{User: "120363420428178043", Server: types.GroupServer}, "120363420428178043@g.us", ""},
		{"broadcast keeps its namespace", types.JID{User: "status", Server: types.BroadcastServer}, "status@broadcast", ""},
		{"empty", types.EmptyJID, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotSender, gotLID := normalizeSender(client, tc.in)
			if gotSender != tc.wantSender {
				t.Errorf("sender: got %q, want %q", gotSender, tc.wantSender)
			}
			if gotLID != tc.wantLID {
				t.Errorf("sender_lid: got %q, want %q", gotLID, tc.wantLID)
			}
		})
	}
}

// The highest-severity QA finding: recordOutgoing and handleMessage must agree on
// the chat key, or one conversation is filed under two chat_jid values. The hard
// case is a LID the local map cannot resolve — recordOutgoing has to fall back to
// the phone JID the send started from, exactly as handleMessage uses RecipientAlt.
func TestRecordOutgoing_ChatKeyMatchesInboundPath(t *testing.T) {
	unmappedLID := types.JID{User: "777666555444333", Server: types.HiddenUserServer}
	phone := types.JID{User: "919060899999", Server: types.DefaultUserServer}

	// Empty LID store: resolution MUST come from the passed alt, not a lookup.
	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	logger := testLogger()

	// Inbound path: whatsmeow supplies the phone form in RecipientAlt.
	handleMessage(client, ms, buildTextMessage(
		unmappedLID, unmappedLID, types.EmptyJID, phone, true, "inbound-side"), logger)

	// Outgoing path: we send to the LID but remember the phone we started from.
	recordOutgoing(client, ms, unmappedLID, phone,
		whatsmeow.SendResponse{ID: "outgoing-1", Timestamp: time.Now()},
		&waProto.Message{Conversation: proto.String("outgoing-side")})

	if got := queryMessageCount(ms, phone.String()); got != 2 {
		t.Errorf("expected both messages under %s, got %d", phone, got)
	}
	if got := queryMessageCount(ms, unmappedLID.String()); got != 0 {
		t.Errorf("conversation split: %d messages filed under the LID %s", got, unmappedLID)
	}
}

// TouchChat used to be UPDATE-only, so the first message in a new conversation
// stored the message but created no chats row.
func TestRecordOutgoing_CreatesChatRowForNewConversation(t *testing.T) {
	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	phone := types.JID{User: "919999999999", Server: types.DefaultUserServer}

	recordOutgoing(client, ms, phone, phone,
		whatsmeow.SendResponse{ID: "first-ever", Timestamp: time.Now()},
		&waProto.Message{Conversation: proto.String("first message to this contact")})

	if _, found := queryChat(ms, phone.String()); !found {
		t.Errorf("no chats row created for a brand-new conversation %s", phone)
	}
	if got := queryMessageCount(ms, phone.String()); got != 1 {
		t.Errorf("expected 1 message, got %d", got)
	}
}

// StoreMessage maintains the FTS5 external-content index by hand. Re-storing the
// same message (a retry, or history sync overlapping live delivery) must leave
// search working rather than pointing at a stale rowid.
func TestStoreMessage_SearchIndexSurvivesRestore(t *testing.T) {
	ms := newTestMessageStore(t)
	chat := "919060899999@s.whatsapp.net"
	if err := ms.TouchChat(chat, time.Now()); err != nil {
		t.Fatalf("TouchChat: %v", err)
	}
	if err := ms.StoreMessage("m1", chat, "919060899999", "811", "vendor update coralogix",
		time.Now(), false, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatalf("first store: %v", err)
	}
	if err := ms.StoreMessage("m1", chat, "919060899999", "811", "vendor update inworld",
		time.Now(), false, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatalf("re-store: %v", err)
	}

	var n int
	if err := ms.db.QueryRow(
		`SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'inworld'`).Scan(&n); err != nil {
		t.Fatalf("fts query: %v", err)
	}
	if n != 1 {
		t.Errorf("expected the updated text to be searchable exactly once, got %d", n)
	}
	if err := ms.db.QueryRow(
		`SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'coralogix'`).Scan(&n); err != nil {
		t.Fatalf("fts query: %v", err)
	}
	if n != 0 {
		t.Errorf("stale text still searchable after update: %d hits", n)
	}
}

// A group legitimately named with digits must keep its name.
func TestIsIdentifierNotName_GroupNamesKept(t *testing.T) {
	group := types.JID{User: "120363420428178043", Server: types.GroupServer}
	dm := types.JID{User: "919060899999", Server: types.DefaultUserServer}

	if isIdentifierNotName("1234567890", group) {
		t.Error("a numeric group subject should be kept")
	}
	if isIdentifierNotName("AD - KR - RR - KC - AK", group) {
		t.Error("a normal group subject should be kept")
	}
	if !isIdentifierNotName("81141293912122", dm) {
		t.Error("a LID stored as a DM contact name should be rejected")
	}
	if isIdentifierNotName("Keshav", dm) {
		t.Error("a real contact name should be kept")
	}
}

// --- Regression tests for the round-2 QA findings ---

// canonicalSender must never guess. E.164 allows up to 15 digits, so the old
// "digits and longer than 13 means LID" rule would permanently relabel a valid
// long phone number as a LID.
func TestCanonicalSender_DoesNotGuessLongNumbers(t *testing.T) {
	lid2pn := map[string]string{"185366493536339": "11234567890"}
	pn2lid := map[string]string{"11234567890": "185366493536339"}

	cases := []struct{ raw, wantSender, wantLID string }{
		{"185366493536339", "11234567890", "185366493536339"},      // known LID
		{"11234567890", "11234567890", "185366493536339"},          // known phone
		{"999888777666555@lid", "999888777666555@lid", "999888777666555"}, // marked, unknown
		{"123456789012345", "123456789012345", ""},                 // 15 digits, unknown: leave alone
		{"12345678901234", "12345678901234", ""},                   // 14 digits, unknown: leave alone
	}
	for _, c := range cases {
		gotS, gotL := canonicalSender(c.raw, "", lid2pn, pn2lid)
		if gotS != c.wantSender || gotL != c.wantLID {
			t.Errorf("canonicalSender(%q) = (%q,%q), want (%q,%q)", c.raw, gotS, gotL, c.wantSender, c.wantLID)
		}
	}
}

// The old guard tested `sender_lid IS NULL` while the migration writes '',
// so it could neither tell done from pending. Second run must be a no-op.
func TestNormalizeExistingSenders_Idempotent(t *testing.T) {
	ms := newTestMessageStore(t)
	logger := testLogger()
	dir := t.TempDir()
	wa := filepath.Join(dir, "whatsapp.db")
	wdb, err := sql.Open("sqlite3", wa)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := wdb.Exec(`CREATE TABLE whatsmeow_lid_map (lid TEXT PRIMARY KEY, pn TEXT UNIQUE NOT NULL);
		INSERT INTO whatsmeow_lid_map VALUES ('185366493536339','11234567890');`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_ = wdb.Close()

	chat := "11234567890@s.whatsapp.net"
	if err := ms.TouchChat(chat, time.Now()); err != nil {
		t.Fatalf("TouchChat: %v", err)
	}
	if err := ms.StoreMessage("m1", chat, "185366493536339", "", "hello",
		time.Now(), false, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatalf("store: %v", err)
	}

	if err := ms.NormalizeExistingSenders(wa, logger); err != nil {
		t.Fatalf("first run: %v", err)
	}
	var sender, senderLID string
	if err := ms.db.QueryRow(`SELECT sender, COALESCE(sender_lid,'') FROM messages WHERE id='m1'`).
		Scan(&sender, &senderLID); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if sender != "11234567890" || senderLID != "185366493536339" {
		t.Fatalf("after first run got (%q,%q), want (11234567890,185366493536339)", sender, senderLID)
	}

	// Second run must find nothing to do. If the guard is wrong it rewrites every
	// mapped sender and rebuilds the index on every single startup.
	before := countSenderWrites(t, ms)
	if err := ms.NormalizeExistingSenders(wa, logger); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if after := countSenderWrites(t, ms); after != before {
		t.Errorf("second run was not a no-op: %q -> %q", before, after)
	}
}

// countSenderWrites is a cheap proxy: the data must be unchanged after a no-op run.
func countSenderWrites(t *testing.T, ms *MessageStore) string {
	t.Helper()
	var s string
	if err := ms.db.QueryRow(`SELECT group_concat(sender||'/'||COALESCE(sender_lid,'')) FROM messages`).Scan(&s); err != nil {
		t.Fatalf("proxy read: %v", err)
	}
	return s
}

// A message stored while the search index did not exist must become searchable
// once it does; an external-content FTS table does not index existing rows.
func TestNewMessageStore_ReconcilesStaleSearchIndex(t *testing.T) {
	ms := newTestMessageStore(t)
	chat := "919060899999@s.whatsapp.net"
	if err := ms.TouchChat(chat, time.Now()); err != nil {
		t.Fatalf("TouchChat: %v", err)
	}
	// Write directly, bypassing StoreMessage's index maintenance, to simulate a
	// row that predates the index.
	if _, err := ms.db.Exec(
		`INSERT INTO messages (id, chat_jid, sender, sender_lid, content, timestamp, is_from_me)
		 VALUES ('old','`+chat+`','919060899999','','murfalcondeck',?,0)`, time.Now()); err != nil {
		t.Fatalf("direct insert: %v", err)
	}

	var indexed int
	if err := ms.db.QueryRow(`SELECT COUNT(*) FROM messages_fts_docsize`).Scan(&indexed); err != nil {
		t.Fatalf("docsize: %v", err)
	}
	if indexed != 0 {
		t.Fatalf("test setup failed to create a stale index: %d indexed", indexed)
	}

	if err := ms.reconcileSearchIndex(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var hits int
	if err := ms.db.QueryRow(
		`SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'murfalcondeck'`).Scan(&hits); err != nil {
		t.Fatalf("search: %v", err)
	}
	if hits != 1 {
		t.Errorf("pre-existing row not searchable after rebuild: %d hits", hits)
	}
}

// --- Round-3 regression tests ---

// canonicalSender must never erase an alias an earlier, better-informed run
// already worked out. Returning "" for an unknown sender wiped sender_lid.
func TestCanonicalSender_PreservesExistingAlias(t *testing.T) {
	empty := map[string]string{}
	gotS, gotL := canonicalSender("12345678901234", "999888777666555", empty, empty)
	if gotS != "12345678901234" || gotL != "999888777666555" {
		t.Errorf("got (%q,%q), want the row left intact (12345678901234,999888777666555)", gotS, gotL)
	}
}

// One sender must be updated once. Grouping by (sender, sender_lid) produced
// several changes for the same sender whose UPDATEs overwrote each other.
func TestNormalizeExistingSenders_SenderWithMixedAliases(t *testing.T) {
	ms := newTestMessageStore(t)
	logger := testLogger()
	dir := t.TempDir()
	wa := filepath.Join(dir, "whatsapp.db")
	wdb, err := sql.Open("sqlite3", wa)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := wdb.Exec(`CREATE TABLE whatsmeow_lid_map (lid TEXT PRIMARY KEY, pn TEXT UNIQUE NOT NULL);
		INSERT INTO whatsmeow_lid_map VALUES ('185366493536339','11234567890');`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_ = wdb.Close()

	chat := "11234567890@s.whatsapp.net"
	if err := ms.TouchChat(chat, time.Now()); err != nil {
		t.Fatalf("TouchChat: %v", err)
	}
	// Same sender, two different stored aliases — the state the old grouping broke on.
	for i, alias := range []string{"", "185366493536339"} {
		if _, err := ms.db.Exec(
			`INSERT INTO messages (id, chat_jid, sender, sender_lid, content, timestamp, is_from_me)
			 VALUES (?,?,?,?,?,?,0)`,
			[]string{"a", "b"}[i], chat, "185366493536339", alias, "hi", time.Now()); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	if err := ms.NormalizeExistingSenders(wa, logger); err != nil {
		t.Fatalf("normalise: %v", err)
	}
	rows, err := ms.db.Query(`SELECT sender, COALESCE(sender_lid,'') FROM messages ORDER BY id`)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer func() { _ = rows.Close() }()
	n := 0
	for rows.Next() {
		var s, l string
		if err := rows.Scan(&s, &l); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if s != "11234567890" || l != "185366493536339" {
			t.Errorf("row %d: got (%q,%q), want (11234567890,185366493536339)", n, s, l)
		}
		n++
	}
	if n != 2 {
		t.Errorf("expected 2 rows, got %d", n)
	}
}

// With no LID map at all there is still map-independent work: an unresolved LID
// must keep its explicit "@lid" marker so it cannot be misread as a phone number.
func TestNormalizeExistingSenders_EmptyMapStillMarksLIDs(t *testing.T) {
	ms := newTestMessageStore(t)
	logger := testLogger()
	dir := t.TempDir()
	wa := filepath.Join(dir, "whatsapp.db")
	wdb, err := sql.Open("sqlite3", wa)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := wdb.Exec(`CREATE TABLE whatsmeow_lid_map (lid TEXT PRIMARY KEY, pn TEXT UNIQUE NOT NULL);`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_ = wdb.Close()

	chat := "11234567890@s.whatsapp.net"
	if err := ms.TouchChat(chat, time.Now()); err != nil {
		t.Fatalf("TouchChat: %v", err)
	}
	if _, err := ms.db.Exec(
		`INSERT INTO messages (id, chat_jid, sender, sender_lid, content, timestamp, is_from_me)
		 VALUES ('x',?, '999888777666555@lid', '', 'hi', ?, 0)`, chat, time.Now()); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := ms.NormalizeExistingSenders(wa, logger); err != nil {
		t.Fatalf("normalise: %v", err)
	}
	var s, l string
	if err := ms.db.QueryRow(`SELECT sender, COALESCE(sender_lid,'') FROM messages WHERE id='x'`).Scan(&s, &l); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if s != "999888777666555@lid" || l != "999888777666555" {
		t.Errorf("got (%q,%q), want (999888777666555@lid,999888777666555)", s, l)
	}
}

// A value claimed by BOTH namespaces is ambiguous; reassigning it would change
// whose identity a row belongs to. Leave it alone.
func TestCanonicalSender_AmbiguousValueIsNotReassigned(t *testing.T) {
	collide := "919948093961"
	lid2pn := map[string]string{collide: "911111111111"} // it is someone's LID
	pn2lid := map[string]string{collide: "222222222222"} // and someone else's phone
	gotS, gotL := canonicalSender(collide, "existing-alias", lid2pn, pn2lid)
	if gotS != collide || gotL != "existing-alias" {
		t.Errorf("got (%q,%q), want the row untouched (%q,existing-alias)", gotS, gotL, collide)
	}
}

// The baseline rebuild must happen once, not on every startup.
func TestReconcileSearchIndex_BaselineIsTakenOnce(t *testing.T) {
	ms := newTestMessageStore(t)
	if _, err := ms.db.Exec(`CREATE TABLE IF NOT EXISTS bridge_meta (key TEXT PRIMARY KEY, value TEXT)`); err != nil {
		t.Fatalf("meta: %v", err)
	}
	if err := ms.reconcileSearchIndex(); err != nil {
		t.Fatalf("first: %v", err)
	}
	var marker string
	if err := ms.db.QueryRow(`SELECT value FROM bridge_meta WHERE key='fts_baseline_rebuild'`).Scan(&marker); err != nil {
		t.Fatalf("marker not recorded: %v", err)
	}
	// Second call must not re-baseline.
	if err := ms.reconcileSearchIndex(); err != nil {
		t.Fatalf("second: %v", err)
	}
	var n int
	if err := ms.db.QueryRow(`SELECT COUNT(*) FROM bridge_meta`).Scan(&n); err != nil || n != 1 {
		t.Errorf("expected exactly one marker row, got %d (err %v)", n, err)
	}
}

// An explicit "@lid" suffix was written by this code, not inferred, so it must
// outrank map membership. Checking the maps first reassigned a suffixed LID as a
// phone number whenever those digits were also somebody's phone.
func TestCanonicalSender_ExplicitSuffixBeatsInference(t *testing.T) {
	value := "919948093961"

	// (a) suffixed, and the same digits are somebody else's phone number.
	gotS, gotL := canonicalSender(value+"@lid", "", map[string]string{}, map[string]string{value: "777"})
	if gotS != value+"@lid" || gotL != value {
		t.Errorf("suffixed + phone collision: got (%q,%q), want (%s@lid,%s)", gotS, gotL, value, value)
	}

	// (b) suffixed and present in both directions: the suffix disambiguates it.
	gotS, gotL = canonicalSender(value+"@lid", "",
		map[string]string{value: "911111111111"}, map[string]string{value: "222222222222"})
	if gotS != "911111111111" || gotL != value {
		t.Errorf("suffixed + ambiguous: got (%q,%q), want (911111111111,%s)", gotS, gotL, value)
	}
}
