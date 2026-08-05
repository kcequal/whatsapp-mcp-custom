package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal"

	"bytes"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// Whether to forward messages sent by self via webhook.
// Defaults to true. Override with env FORWARD_SELF=false.
var forwardSelfMessages = getEnvBool("FORWARD_SELF", true)

// getEnvBool reads a boolean env var with a default.
// Accepts: 1/true/yes/on and 0/false/no/off (case-insensitive)
func getEnvBool(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

// Message represents a chat message for our client
type Message struct {
	Time      time.Time
	Sender    string
	Content   string
	IsFromMe  bool
	MediaType string
	Filename  string
}

// Database handler for storing message history
type MessageStore struct {
	db *sql.DB
	// ftsEnabled is false when the binary was built without the sqlite_fts5 tag.
	// Building without it has silently broken every message write before now, so
	// we detect it once at startup, say so loudly, and degrade to "no search"
	// rather than "no messages".
	ftsEnabled bool
}

// Initialize message store
func NewMessageStore() (*MessageStore, error) {
	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}

	// Open SQLite database for messages
	// WAL + a busy timeout, because two goroutines write here concurrently:
	// handleMessage on inbound events and recordOutgoing on our own sends.
	// *sql.DB is a connection pool, not a lock, so without these one writer
	// gets SQLITE_BUSY and the message is dropped with only a log line.
	db, err := sql.Open("sqlite3",
		"file:store/messages.db?_foreign_keys=on&_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("failed to open message database: %v", err)
	}
	// One writer at a time. SQLite allows a single writer regardless; serialising
	// in the pool turns lock contention into a short wait instead of an error.
	db.SetMaxOpenConns(1)

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS messages (
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

		CREATE TABLE IF NOT EXISTS calls (
			call_id TEXT,
			caller_jid TEXT,
			from_jid TEXT,
			event_type TEXT,
			media TEXT,
			is_group BOOLEAN,
			timestamp TIMESTAMP,
			PRIMARY KEY (call_id, event_type)
		);
	`)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	// The search index is part of the schema, not an optional extra the Python
	// layer bolts on. StoreMessage maintains it on every write, so a database
	// without it fails every insert.
	// Probe FTS5 properly. CREATE VIRTUAL TABLE IF NOT EXISTS is NOT a capability
	// check: if messages_fts already exists (as it does on every live box) the
	// statement succeeds without ever loading the module, so a binary built
	// without -tags sqlite_fts5 would pass this check and then fail every single
	// write inside StoreMessage. A throwaway temp table is an honest probe.
	ftsEnabled := true
	if _, probeErr := db.Exec(`CREATE VIRTUAL TABLE temp.kcbridge_fts5_probe USING fts5(x)`); probeErr != nil {
		if strings.Contains(probeErr.Error(), "no such module: fts5") {
			ftsEnabled = false
			fmt.Println("*** WARNING: built WITHOUT the sqlite_fts5 build tag.        ***")
			fmt.Println("*** Messages are stored but NOT indexed for search.          ***")
			fmt.Println("*** Rebuild with:  go build -tags sqlite_fts5                ***")
		} else {
			_ = db.Close()
			return nil, fmt.Errorf("failed to probe FTS5 support: %v", probeErr)
		}
	} else if _, dropErr := db.Exec(`DROP TABLE temp.kcbridge_fts5_probe`); dropErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to drop FTS5 probe table: %v", dropErr)
	}

	if ftsEnabled {
		if _, ftsErr := db.Exec(`
			CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
				content, sender, chat_jid,
				content='messages',
				content_rowid='rowid',
				tokenize='unicode61 remove_diacritics 2'
			);
		`); ftsErr != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to create search index: %v", ftsErr)
		}
	}

	store := &MessageStore{db: db, ftsEnabled: ftsEnabled}
	if err := store.reconcileSearchIndex(); err != nil {
		_ = db.Close()
		return nil, err
	}

	// Migration: sender_lid was added 2026-08-06 alongside sender normalisation.
	// CREATE TABLE IF NOT EXISTS won't add it to an existing database, so do it
	// here. "duplicate column name" just means we've already run.
	if _, alterErr := db.Exec(`ALTER TABLE messages ADD COLUMN sender_lid TEXT`); alterErr != nil &&
		!strings.Contains(alterErr.Error(), "duplicate column name") {
		_ = db.Close()
		return nil, fmt.Errorf("failed to add sender_lid column: %v", alterErr)
	}

	return store, nil
}

// NormalizeExistingSenders rewrites historical `sender` values to the canonical
// phone form and backfills `sender_lid`, using the whatsmeow LID map.
//
// This exists in code, not as a one-off script, because a script only fixes the
// box it was run on. This bridge also runs on Keshav's machine, which would
// otherwise get the new column and none of the normalisation — the same
// per-box-fix trap that has bitten this setup before.
//
// Idempotent: rows already in canonical form are rewritten to themselves.
// Rebuilds the FTS index afterwards, because `sender` is an indexed FTS column
// and a bulk UPDATE leaves the external-content index stale.
func (store *MessageStore) NormalizeExistingSenders(whatsappDBPath string, logger waLog.Logger) error {
	if _, err := os.Stat(whatsappDBPath); err != nil {
		if os.IsNotExist(err) {
			logger.Infof("Skipping sender normalisation: %s not found", whatsappDBPath)
			return nil
		}
		return fmt.Errorf("failed to stat WhatsApp DB %s: %w", whatsappDBPath, err)
	}

	// Load the WHOLE map before touching anything. A partially-read map would
	// classify known people as unknown and write that guess to disk, so any scan
	// or iteration error has to abort before the first write, not be skipped.
	lid2pn, pn2lid, err := loadLIDMap(whatsappDBPath)
	if err != nil {
		return fmt.Errorf("sender normalisation aborted, LID map unreadable: %w", err)
	}
	// Deliberately no early return on an empty map: marking an unresolved LID
	// with its "@lid" suffix is map-independent work that still needs doing.

	// Everything below runs inside ONE transaction: enumerate, decide, update,
	// reindex. Enumerating outside it would let a sender that appears in between
	// be silently skipped while the run still reports success. (No writer is
	// running at this point in startup, but correctness should not depend on
	// that staying true.)
	//
	// No pre-count guard. The previous one tested `sender_lid IS NULL` while this
	// function writes '', so completed rows looked pending and pending rows looked
	// done. Comparing desired against stored is exact, and the sender set is
	// hundreds of rows, not millions.
	tx, err := store.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Group by sender alone: the desired pair is a function of the sender string,
	// so grouping by (sender, sender_lid) produced several changes for one sender
	// whose UPDATEs then overwrote each other.
	rows, err := tx.Query(`SELECT DISTINCT sender FROM messages WHERE sender != ''`)
	if err != nil {
		return fmt.Errorf("failed to list senders: %w", err)
	}
	var senders []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			_ = rows.Close()
			return fmt.Errorf("failed to read sender row: %w", err)
		}
		senders = append(senders, s)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("failed while reading senders: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close sender query: %w", err)
	}

	changed := 0
	for _, raw := range senders {
		// The existing alias for this sender, if the rows agree on one.
		var currentLID string
		if err := tx.QueryRow(
			`SELECT COALESCE(MAX(sender_lid), '') FROM messages WHERE sender = ? AND COALESCE(sender_lid,'') != ''`,
			raw).Scan(&currentLID); err != nil {
			return fmt.Errorf("failed to read current alias for %q: %w", raw, err)
		}
		want, wantLID := canonicalSender(raw, currentLID, lid2pn, pn2lid)

		// Update only rows that actually differ, so a repeat run touches nothing.
		res, err := tx.Exec(
			`UPDATE messages SET sender = ?, sender_lid = ?
			 WHERE sender = ? AND (sender != ? OR COALESCE(sender_lid,'') != ?)`,
			want, wantLID, raw, want, wantLID)
		if err != nil {
			return fmt.Errorf("failed to normalise sender %q: %w", raw, err)
		}
		if n, err := res.RowsAffected(); err == nil && n > 0 {
			changed++
		}
	}

	if changed == 0 {
		logger.Infof("Sender normalisation: already canonical")
		return tx.Commit()
	}

	// `sender` is an indexed FTS column and the UPDATEs above bypassed the index,
	// so rebuild in the same transaction: it sees these updates, and a failure
	// rolls everything back together.
	if store.ftsEnabled {
		if _, err := tx.Exec(`INSERT INTO messages_fts(messages_fts) VALUES('rebuild')`); err != nil {
			return fmt.Errorf("failed to rebuild search index after normalisation: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	logger.Infof("Sender normalisation complete: %d distinct senders rewritten", changed)
	return nil
}

// canonicalSender is the batch equivalent of normalizeSender, working off a
// loaded LID map instead of the live client.
//
// It deliberately does NOT guess. An earlier version called any run of digits
// longer than 13 a LID; E.164 allows up to 15, so a legitimate 14- or 15-digit
// phone number would have been permanently relabelled. An unmarked value that
// the map does not know stays exactly as it is, and gets another chance on a
// later run once the map learns it.
func canonicalSender(raw, currentLID string, lid2pn, pn2lid map[string]string) (string, string) {
	bare := strings.Split(strings.Split(raw, "@")[0], ":")[0]
	if pn, ok := lid2pn[bare]; ok {
		return pn, bare
	}
	if lid, ok := pn2lid[bare]; ok {
		return bare, lid
	}
	if strings.HasSuffix(raw, "@lid") {
		return bare + "@lid", bare
	}
	// Nothing authoritative. Leave the row exactly as it is — returning an empty
	// alias here would ERASE a sender_lid that an earlier, better-informed run
	// had already worked out.
	return raw, currentLID
}

// loadLIDMap reads the whole whatsmeow LID/phone mapping, or fails.
func loadLIDMap(whatsappDBPath string) (lid2pn, pn2lid map[string]string, err error) {
	wdb, err := sql.Open("sqlite3", "file:"+whatsappDBPath+"?mode=ro")
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = wdb.Close() }()

	rows, err := wdb.Query("SELECT lid, pn FROM whatsmeow_lid_map")
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()

	lid2pn = map[string]string{}
	pn2lid = map[string]string{}
	for rows.Next() {
		var lid, pn string
		if err := rows.Scan(&lid, &pn); err != nil {
			return nil, nil, err
		}
		lid = strings.Split(strings.Split(lid, "@")[0], ":")[0]
		pn = strings.Split(strings.Split(pn, "@")[0], ":")[0]
		lid2pn[lid] = pn
		pn2lid[pn] = lid
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	return lid2pn, pn2lid, nil
}

// reconcileSearchIndex rebuilds messages_fts when it has fallen behind the
// messages table.
//
// An external-content FTS5 table does not index rows that already exist, so a
// freshly created index starts empty; and anything written while the binary
// lacked FTS5 support is missing from it too. Either way search is silently
// incomplete and StoreMessage's retract step assumes entries that were never
// there.
//
// The obvious check does not work: COUNT(*) on the FTS table reads through to
// the content table, so it always equals COUNT(*) on messages even when nothing
// is indexed. The shadow table messages_fts_docsize holds one row per genuinely
// indexed document, which is the real number.
func (store *MessageStore) reconcileSearchIndex() error {
	if !store.ftsEnabled {
		return nil
	}
	var msgCount, indexed int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&msgCount); err != nil {
		return fmt.Errorf("failed to count messages: %w", err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM messages_fts_docsize`).Scan(&indexed); err != nil {
		// An index built with columnsize=0 has no docsize shadow table, and a
		// rebuild will not create one — so treating "cannot tell" as "rebuild"
		// would rebuild the whole index on every single startup forever. Say so
		// once and leave the index alone; degraded search beats a boot-time
		// rebuild loop.
		fmt.Printf("Cannot determine search index freshness (%v); skipping reconciliation\n", err)
		return nil
	}
	if indexed == msgCount {
		return nil
	}
	fmt.Printf("Search index out of sync (%d indexed vs %d messages), rebuilding\n", indexed, msgCount)
	if _, err := store.db.Exec(`INSERT INTO messages_fts(messages_fts) VALUES('rebuild')`); err != nil {
		return fmt.Errorf("failed to rebuild search index: %w", err)
	}
	return nil
}

// MigrateLegacyLIDChatsToPhoneJIDs rewrites message/chat rows stored under
// legacy @lid chat JIDs into phone-based @s.whatsapp.net chat JIDs using the
// whatsmeow LID map in whatsapp.db.
func (store *MessageStore) MigrateLegacyLIDChatsToPhoneJIDs(whatsappDBPath string, logger waLog.Logger) error {
	if _, err := os.Stat(whatsappDBPath); err != nil {
		if os.IsNotExist(err) {
			logger.Infof("Skipping LID chat migration: %s not found", whatsappDBPath)
			return nil
		}
		return fmt.Errorf("failed to stat WhatsApp DB %s: %w", whatsappDBPath, err)
	}

	tx, err := store.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start LID chat migration transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	alias := fmt.Sprintf("wa_mig_%d", time.Now().UnixNano())
	escapedPath := strings.ReplaceAll(whatsappDBPath, "'", "''")
	if _, err := tx.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS %s;", escapedPath, alias)); err != nil {
		return fmt.Errorf("failed to attach WhatsApp DB for LID chat migration: %w", err)
	}

	var lidMapTableExists int
	if err := tx.QueryRow(fmt.Sprintf(
		"SELECT COUNT(1) FROM %s.sqlite_master WHERE type='table' AND name='whatsmeow_lid_map';",
		alias,
	)).Scan(&lidMapTableExists); err != nil {
		return fmt.Errorf("failed to inspect WhatsApp DB schema for LID migration: %w", err)
	}
	if lidMapTableExists == 0 {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit no-op LID chat migration: %w", err)
		}
		logger.Infof("Skipping LID chat migration: whatsmeow_lid_map table not found")
		return nil
	}

	if _, err := tx.Exec(fmt.Sprintf(`
		CREATE TEMP TABLE tmp_lid_to_phone AS
		SELECT DISTINCT
			lm.lid || '@lid' AS lid_jid,
			lm.pn || '@s.whatsapp.net' AS phone_jid
		FROM %s.whatsmeow_lid_map lm
		WHERE lm.lid != '' AND lm.pn != ''
		  AND (
		  	EXISTS (SELECT 1 FROM chats c WHERE c.jid = lm.lid || '@lid')
		  	OR EXISTS (SELECT 1 FROM messages m WHERE m.chat_jid = lm.lid || '@lid')
		  );
	`, alias)); err != nil {
		return fmt.Errorf("failed to build temporary LID mapping table: %w", err)
	}

	var mappedChats int
	if err := tx.QueryRow("SELECT COUNT(*) FROM tmp_lid_to_phone;").Scan(&mappedChats); err != nil {
		return fmt.Errorf("failed to count mapped LID chats: %w", err)
	}

	if mappedChats == 0 {
		if _, err := tx.Exec("DROP TABLE IF EXISTS tmp_lid_to_phone;"); err != nil {
			return fmt.Errorf("failed to clean temporary LID mapping table: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit no-op LID chat migration: %w", err)
		}
		logger.Infof("LID chat migration: nothing to migrate")
		return nil
	}

	if _, err := tx.Exec(`
		CREATE TEMP TABLE tmp_lid_chat_candidates AS
		SELECT
			m.phone_jid AS phone_jid,
			m.lid_jid AS lid_jid,
			NULLIF(TRIM(c.name), '') AS source_name,
			COALESCE(
				c.last_message_time,
				(
					SELECT MAX(msg.timestamp)
					FROM messages msg
					WHERE msg.chat_jid = m.lid_jid
				)
			) AS source_last_message_time
		FROM tmp_lid_to_phone m
		LEFT JOIN chats c ON c.jid = m.lid_jid;
	`); err != nil {
		return fmt.Errorf("failed to build temporary chat candidate table: %w", err)
	}

	if _, err := tx.Exec(`
		CREATE TEMP TABLE tmp_lid_chat_meta AS
		SELECT
			c.phone_jid AS phone_jid,
			COALESCE(
				(
					SELECT c2.source_name
					FROM tmp_lid_chat_candidates c2
					WHERE c2.phone_jid = c.phone_jid
						AND c2.source_name IS NOT NULL
					ORDER BY
						CASE WHEN c2.source_last_message_time IS NULL THEN 1 ELSE 0 END,
						c2.source_last_message_time DESC,
						c2.lid_jid ASC
					LIMIT 1
				),
				substr(c.phone_jid, 1, instr(c.phone_jid, '@') - 1)
			) AS source_name,
			MAX(c.source_last_message_time) AS source_last_message_time
		FROM tmp_lid_chat_candidates c
		GROUP BY c.phone_jid;
	`); err != nil {
		return fmt.Errorf("failed to build temporary chat metadata table: %w", err)
	}

	if _, err := tx.Exec(`
		INSERT OR IGNORE INTO chats (jid, name, last_message_time)
		SELECT phone_jid, source_name, source_last_message_time
		FROM tmp_lid_chat_meta;
	`); err != nil {
		return fmt.Errorf("failed to upsert destination chat rows: %w", err)
	}

	if _, err := tx.Exec(`
		UPDATE chats
		SET
			name = CASE
				WHEN (name IS NULL OR TRIM(name) = '') THEN (
					SELECT m.source_name
					FROM tmp_lid_chat_meta m
					WHERE m.phone_jid = chats.jid
				)
				ELSE name
			END,
			last_message_time = CASE
				WHEN (
					SELECT m.source_last_message_time
					FROM tmp_lid_chat_meta m
					WHERE m.phone_jid = chats.jid
				) IS NULL THEN last_message_time
				WHEN last_message_time IS NULL THEN (
					SELECT m.source_last_message_time
					FROM tmp_lid_chat_meta m
					WHERE m.phone_jid = chats.jid
				)
				WHEN (
					SELECT m.source_last_message_time
					FROM tmp_lid_chat_meta m
					WHERE m.phone_jid = chats.jid
				) > last_message_time THEN (
					SELECT m.source_last_message_time
					FROM tmp_lid_chat_meta m
					WHERE m.phone_jid = chats.jid
				)
				ELSE last_message_time
			END
		WHERE jid IN (SELECT phone_jid FROM tmp_lid_chat_meta);
	`); err != nil {
		return fmt.Errorf("failed to merge destination chat metadata: %w", err)
	}

	insertResult, err := tx.Exec(`
		INSERT OR IGNORE INTO messages (
			id, chat_jid, sender, content, timestamp, is_from_me,
			media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length
		)
		SELECT
			msg.id,
			m.phone_jid,
			msg.sender,
			msg.content,
			msg.timestamp,
			msg.is_from_me,
			msg.media_type,
			msg.filename,
			msg.url,
			msg.media_key,
			msg.file_sha256,
			msg.file_enc_sha256,
			msg.file_length
		FROM messages msg
		JOIN tmp_lid_to_phone m ON m.lid_jid = msg.chat_jid;
	`)
	if err != nil {
		return fmt.Errorf("failed to copy legacy LID messages into phone chats: %w", err)
	}

	insertedMessages, _ := insertResult.RowsAffected()

	deleteMessagesResult, err := tx.Exec(`
		DELETE FROM messages
		WHERE chat_jid IN (SELECT lid_jid FROM tmp_lid_to_phone);
	`)
	if err != nil {
		return fmt.Errorf("failed to delete migrated LID messages: %w", err)
	}
	deletedMessages, _ := deleteMessagesResult.RowsAffected()

	deleteChatsResult, err := tx.Exec(`
		DELETE FROM chats
		WHERE jid IN (SELECT lid_jid FROM tmp_lid_to_phone);
	`)
	if err != nil {
		return fmt.Errorf("failed to delete migrated LID chats: %w", err)
	}
	deletedChats, _ := deleteChatsResult.RowsAffected()

	if _, err := tx.Exec("DROP TABLE IF EXISTS tmp_lid_to_phone;"); err != nil {
		return fmt.Errorf("failed to clean temporary LID mapping table: %w", err)
	}
	if _, err := tx.Exec("DROP TABLE IF EXISTS tmp_lid_chat_meta;"); err != nil {
		return fmt.Errorf("failed to clean temporary chat metadata table: %w", err)
	}
	if _, err := tx.Exec("DROP TABLE IF EXISTS tmp_lid_chat_candidates;"); err != nil {
		return fmt.Errorf("failed to clean temporary chat candidate table: %w", err)
	}

	// This migration DELETEs and re-INSERTs message rows, which changes their
	// rowids and rewrites chat_jid — both indexed by messages_fts, which has no
	// triggers. Rebuild inside the same transaction, or the search index is left
	// pointing at rowids that no longer exist.
	//
	// A count-based freshness check cannot catch this: N rows deleted and N
	// reinserted leaves the document count identical while every entry is stale.
	// So the rebuild has to be unconditional whenever this migration changed
	// anything, not deferred to reconcileSearchIndex.
	if store.ftsEnabled && (insertedMessages > 0 || deletedMessages > 0 || deletedChats > 0) {
		if _, err := tx.Exec(`INSERT INTO messages_fts(messages_fts) VALUES('rebuild')`); err != nil {
			return fmt.Errorf("failed to rebuild search index after LID chat migration: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit LID chat migration: %w", err)
	}

	logger.Infof(
		"LID chat migration complete: mapped_chats=%d inserted_messages=%d deleted_lid_messages=%d deleted_lid_chats=%d",
		mappedChats,
		insertedMessages,
		deletedMessages,
		deletedChats,
	)
	return nil
}

// Close the database connection
func (store *MessageStore) Close() error {
	return store.db.Close()
}

// Store a chat in the database
func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		"INSERT OR REPLACE INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
		jid, name, lastMessageTime,
	)
	return err
}

// RenameChat updates only the display name, leaving last_message_time alone.
func (store *MessageStore) RenameChat(jid, name string) error {
	_, err := store.db.Exec("UPDATE chats SET name = ? WHERE jid = ?", name, jid)
	return err
}

// TouchChat bumps a chat's last_message_time without touching its name.
// StoreChat is INSERT OR REPLACE, so calling it from the outgoing path (where
// we have no display name to hand) would blank an existing chat's name.
func (store *MessageStore) TouchChat(jid string, lastMessageTime time.Time) error {
	// Upsert, not UPDATE: this used to be update-only, so the first message in a
	// brand-new conversation stored the message but created no chats row at all —
	// and messages.chat_jid has a foreign key onto chats(jid). Inserting with an
	// empty name is correct here; the outgoing path has no display name to offer
	// and GetChatName treats empty as "resolve me".
	_, err := store.db.Exec(
		`INSERT INTO chats (jid, name, last_message_time) VALUES (?, '', ?)
		 ON CONFLICT(jid) DO UPDATE SET last_message_time = excluded.last_message_time
		 WHERE chats.last_message_time IS NULL OR chats.last_message_time < excluded.last_message_time`,
		jid, lastMessageTime,
	)
	return err
}

// Store a message in the database.
// sender is the canonical identity (phone number where known); senderLID is the
// LID alias, kept because some contacts only ever appear as a LID. See
// normalizeSender for how the pair is derived.
func (store *MessageStore) StoreMessage(id, chatJID, sender, senderLID, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	// Only store if there's actual content or media
	if content == "" && mediaType == "" {
		return nil
	}

	tx, err := store.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// messages_fts is an FTS5 external-content index over (content, sender,
	// chat_jid) with NO triggers, so every write here has to maintain it by hand.
	//
	// Two traps. First, INSERT OR REPLACE deletes and reinserts, which assigns a
	// NEW rowid — the FTS entry then points at a rowid that no longer exists and
	// search silently rots. Second, an external-content 'delete' must be given the
	// OLD column values, not the new ones, or the index is left corrupt. So: read
	// the existing row, retract it from the index, UPSERT (which preserves rowid),
	// then index the new values.
	var oldRowID sql.NullInt64
	var oldContent, oldSender, oldChat string
	if store.ftsEnabled {
		err = tx.QueryRow(
		"SELECT rowid, content, sender, chat_jid FROM messages WHERE id = ? AND chat_jid = ?",
			id, chatJID,
		).Scan(&oldRowID, &oldContent, &oldSender, &oldChat)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if oldRowID.Valid {
			if _, err := tx.Exec(
				`INSERT INTO messages_fts(messages_fts, rowid, content, sender, chat_jid) VALUES('delete', ?, ?, ?, ?)`,
				oldRowID.Int64, oldContent, oldSender, oldChat,
			); err != nil {
				return fmt.Errorf("failed to retract old FTS entry: %w", err)
			}
		}
	}

	if _, err := tx.Exec(
		`INSERT INTO messages
		(id, chat_jid, sender, sender_lid, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id, chat_jid) DO UPDATE SET
			sender=excluded.sender, sender_lid=excluded.sender_lid, content=excluded.content,
			timestamp=excluded.timestamp, is_from_me=excluded.is_from_me, media_type=excluded.media_type,
			filename=excluded.filename, url=excluded.url, media_key=excluded.media_key,
			file_sha256=excluded.file_sha256, file_enc_sha256=excluded.file_enc_sha256,
			file_length=excluded.file_length`,
		id, chatJID, sender, senderLID, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	); err != nil {
		return err
	}

	if store.ftsEnabled {
		var newRowID int64
		if err := tx.QueryRow(
			"SELECT rowid FROM messages WHERE id = ? AND chat_jid = ?", id, chatJID,
		).Scan(&newRowID); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`INSERT INTO messages_fts(rowid, content, sender, chat_jid) VALUES(?, ?, ?, ?)`,
			newRowID, content, sender, chatJID,
		); err != nil {
			return fmt.Errorf("failed to index message for search: %w", err)
		}
	}

	return tx.Commit()
}

// GetOldestMessage returns the earliest known message in a chat — used as
// the anchor for /api/history_sync (whatsmeow's BuildHistorySyncRequest
// needs the oldest known message; the server then returns N older than that).
func (store *MessageStore) GetOldestMessage(chatJID string) (id, sender string, isFromMe bool, ts time.Time, err error) {
	err = store.db.QueryRow(
		"SELECT id, sender, is_from_me, timestamp FROM messages WHERE chat_jid=? ORDER BY timestamp ASC LIMIT 1",
		chatJID,
	).Scan(&id, &sender, &isFromMe, &ts)
	return
}

// Get messages from a chat
func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		"SELECT sender, content, timestamp, is_from_me, media_type, filename FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT ?",
		chatJID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var messages []Message
	for rows.Next() {
		var msg Message
		var timestamp time.Time
		err := rows.Scan(&msg.Sender, &msg.Content, &timestamp, &msg.IsFromMe, &msg.MediaType, &msg.Filename)
		if err != nil {
			return nil, err
		}
		msg.Time = timestamp
		messages = append(messages, msg)
	}

	return messages, nil
}

// logCallEvent persists a single WhatsApp call event into the local store.
// Called from the AddEventHandler closure for *events.CallOffer / *events.CallAccept / etc.
func logCallEvent(store *MessageStore, callID, callerJID, fromJID, eventType, media string, isGroup bool) {
	if store == nil || callID == "" {
		return
	}
	_, err := store.db.Exec(
		"INSERT OR REPLACE INTO calls (call_id, caller_jid, from_jid, event_type, media, is_group, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
		callID, callerJID, fromJID, eventType, media, isGroup, time.Now(),
	)
	if err != nil {
		fmt.Printf("logCallEvent: failed to insert call %s/%s: %v\n", callID, eventType, err)
	}
}

// Get all chats
func (store *MessageStore) GetChats() (map[string]time.Time, error) {
	rows, err := store.db.Query("SELECT jid, last_message_time FROM chats ORDER BY last_message_time DESC")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	chats := make(map[string]time.Time)
	for rows.Next() {
		var jid string
		var lastMessageTime time.Time
		err := rows.Scan(&jid, &lastMessageTime)
		if err != nil {
			return nil, err
		}
		chats[jid] = lastMessageTime
	}

	return chats, nil
}

// Match @<digits> mention tokens in message text. Group members in WhatsApp
// are addressed by LID (linked identifier) — a long numeric string. Mentions
// only render as a clickable tag when the JID is ALSO present in the
// ContextInfo.MentionedJid array; just typing @<lid> in text shows raw.
var mentionRe = regexp.MustCompile(`@(\d{6,})`)

// extractMentions returns the @<digits> mentions in `text` as fully-qualified
// LID JIDs. Empty slice if no mentions found. Caller assigns the result to
// ContextInfo.MentionedJid so receiving clients render clickable tags.
func extractMentions(text string) []string {
	matches := mentionRe.FindAllStringSubmatch(text, -1)
	if len(matches) == 0 {
		return nil
	}
	out := make([]string, 0, len(matches))
	seen := make(map[string]bool, len(matches))
	for _, m := range matches {
		jid := m[1] + "@lid"
		if !seen[jid] {
			seen[jid] = true
			out = append(out, jid)
		}
	}
	return out
}

// Extract text content from a message
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}

	// Try to get text content
	if text := msg.GetConversation(); text != "" {
		return text
	} else if extendedText := msg.GetExtendedTextMessage(); extendedText != nil {
		return extendedText.GetText()
	}

	// Media captions are text too. Without these, an image posted with a
	// paragraph of context stores as an empty row: the daily cost digests all
	// landed as media_type='image' with no content, so "what did we send them"
	// and any keyword search over media messages came back empty.
	if img := msg.GetImageMessage(); img != nil {
		return img.GetCaption()
	} else if vid := msg.GetVideoMessage(); vid != nil {
		return vid.GetCaption()
	} else if doc := msg.GetDocumentMessage(); doc != nil {
		if cap := doc.GetCaption(); cap != "" {
			return cap
		}
		// A document usually has no caption; its filename is the only text
		// that says what it is.
		return doc.GetFileName()
	}

	return ""
}

// SendMessageResponse represents the response for the send message API
type SendMessageResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// SendMessageRequest represents the request body for the send message API
type SendMessageRequest struct {
	Recipient string `json:"recipient"`
	Message   string `json:"message"`
	MediaPath string `json:"media_path,omitempty"`
}

// Function to send a WhatsApp message
func sendWhatsAppMessage(client *whatsmeow.Client, messageStore *MessageStore, recipient string, message string, mediaPath string) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}

	// Create JID for recipient
	var recipientJID types.JID
	var err error

	// Check if recipient is a JID
	isJID := strings.Contains(recipient, "@")

	if isJID {
		// Parse the JID string
		recipientJID, err = types.ParseJID(recipient)
		if err != nil {
			return false, fmt.Sprintf("Error parsing JID: %v", err)
		}
	} else {
		// Create JID from phone number
		recipientJID = types.JID{
			User:   recipient,
			Server: "s.whatsapp.net", // For personal chats
		}
	}

	// For personal chats, resolve phone number JID to LID (Linked Identity).
	// WhatsApp is migrating to LID-based addressing; messages sent to the
	// phone JID silently fail for migrated contacts.
	// Remember the phone form before we swap it for a LID — recordOutgoing needs
	// it to key the chat the same way the inbound path does.
	var recipientAlt types.JID
	if recipientJID.Server == types.DefaultUserServer {
		recipientAlt = recipientJID.ToNonAD()
		ctx := context.Background()
		lid, lidErr := client.Store.LIDs.GetLIDForPN(ctx, recipientJID)
		if lidErr == nil && !lid.IsEmpty() {
			fmt.Printf("Resolved %s -> %s (LID)\n", recipientJID, lid)
			recipientJID = lid
		} else {
			// Cache miss or cache error — ask the WhatsApp server.
			if lidErr != nil {
				fmt.Printf("Warning: LID cache lookup failed for %s: %v, falling back to server\n", recipientJID, lidErr)
			}
			info, infoErr := client.GetUserInfo(ctx, []types.JID{recipientJID})
			if infoErr != nil {
				fmt.Printf("Warning: server LID lookup failed for %s: %v\n", recipientJID, infoErr)
			} else if userInfo, ok := info[recipientJID]; ok && !userInfo.LID.IsEmpty() {
				fmt.Printf("Resolved %s -> %s (LID via server)\n", recipientJID, userInfo.LID)
				recipientJID = userInfo.LID
			}
		}
	}

	msg := &waProto.Message{}

	// Check if we have media to send
	if mediaPath != "" {
		// Read media file
		mediaData, err := os.ReadFile(mediaPath)
		if err != nil {
			return false, fmt.Sprintf("Error reading media file: %v", err)
		}

		// Determine media type and mime type based on file extension
		fileExt := strings.ToLower(mediaPath[strings.LastIndex(mediaPath, ".")+1:])
		var mediaType whatsmeow.MediaType
		var mimeType string

		// Handle different media types
		switch fileExt {
		// Image types
		case "jpg", "jpeg":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/jpeg"
		case "png":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/png"
		case "gif":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/gif"
		case "webp":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/webp"

		// Audio types
		case "ogg":
			mediaType = whatsmeow.MediaAudio
			mimeType = "audio/ogg; codecs=opus"

		// Video types
		case "mp4":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/mp4"
		case "avi":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/avi"
		case "mov":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/quicktime"

		// Document types — set explicit MIME so receivers preview correctly
		case "pdf":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/pdf"
		case "docx":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
		case "doc":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/msword"
		case "xlsx":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
		case "xls":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/vnd.ms-excel"
		case "pptx":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
		case "ppt":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/vnd.ms-powerpoint"
		case "txt":
			mediaType = whatsmeow.MediaDocument
			mimeType = "text/plain"
		case "csv":
			mediaType = whatsmeow.MediaDocument
			mimeType = "text/csv"
		case "md":
			mediaType = whatsmeow.MediaDocument
			mimeType = "text/markdown"
		case "json":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/json"

		// Document types (for any other file type)
		default:
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/octet-stream"
		}

		// Upload media to WhatsApp servers
		resp, err := client.Upload(context.Background(), mediaData, mediaType)
		if err != nil {
			return false, fmt.Sprintf("Error uploading media: %v", err)
		}

		fmt.Println("Media uploaded", resp)

		// Create the appropriate message type based on media type
		switch mediaType {
		case whatsmeow.MediaImage:
			msg.ImageMessage = &waProto.ImageMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaAudio:
			// Handle ogg audio files
			var seconds uint32 = 30 // Default fallback
			var waveform []byte = nil

			// Try to analyze the ogg file
			if strings.Contains(mimeType, "ogg") {
				analyzedSeconds, analyzedWaveform, err := analyzeOggOpus(mediaData)
				if err == nil {
					seconds = analyzedSeconds
					waveform = analyzedWaveform
				} else {
					return false, fmt.Sprintf("Failed to analyze Ogg Opus file: %v", err)
				}
			} else {
				fmt.Printf("Not an Ogg Opus file: %s\n", mimeType)
			}

			msg.AudioMessage = &waProto.AudioMessage{
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
				Seconds:       proto.Uint32(seconds),
				PTT:           proto.Bool(true),
				Waveform:      waveform,
			}
		case whatsmeow.MediaVideo:
			msg.VideoMessage = &waProto.VideoMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaDocument:
			docFileName := mediaPath[strings.LastIndex(mediaPath, "/")+1:]
			msg.DocumentMessage = &waProto.DocumentMessage{
				FileName:      proto.String(docFileName),
				Title:         proto.String(docFileName),
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		}
	} else {
		// If the text contains @<lid> mentions, send as ExtendedTextMessage
		// with MentionedJid populated so receiving clients render clickable
		// tags. Without this, the @-tag shows as raw text. Plain Conversation
		// has no field for mentions.
		if mentions := extractMentions(message); len(mentions) > 0 {
			msg.ExtendedTextMessage = &waProto.ExtendedTextMessage{
				Text: proto.String(message),
				ContextInfo: &waProto.ContextInfo{
					MentionedJID: mentions,
				},
			}
		} else {
			msg.Conversation = proto.String(message)
		}
	}

	// Send message
	resp, err := client.SendMessage(context.Background(), recipientJID, msg)

	if err != nil {
		return false, fmt.Sprintf("Error sending message: %v", err)
	}

	recordOutgoing(client, messageStore, recipientJID, recipientAlt, resp, msg)

	return true, fmt.Sprintf("Message sent to %s", recipient)
}

// Extract quoted message info from ContextInfo
func extractQuotedMessageInfo(msg *waProto.Message) (quotedMessageId string, quotedSender string, quotedContent string) {
	if msg == nil {
		return "", "", ""
	}

	var contextInfo *waProto.ContextInfo

	// Check all message types that can have ContextInfo
	if extText := msg.GetExtendedTextMessage(); extText != nil {
		contextInfo = extText.GetContextInfo()
	} else if img := msg.GetImageMessage(); img != nil {
		contextInfo = img.GetContextInfo()
	} else if vid := msg.GetVideoMessage(); vid != nil {
		contextInfo = vid.GetContextInfo()
	} else if doc := msg.GetDocumentMessage(); doc != nil {
		contextInfo = doc.GetContextInfo()
	} else if aud := msg.GetAudioMessage(); aud != nil {
		contextInfo = aud.GetContextInfo()
	}

	if contextInfo == nil {
		return "", "", ""
	}

	// Extract quoted message ID (StanzaID)
	if contextInfo.StanzaID != nil {
		quotedMessageId = *contextInfo.StanzaID
	}

	// Extract quoted sender (Participant)
	if contextInfo.Participant != nil {
		quotedSender = *contextInfo.Participant
	}

	// Extract quoted message content
	if quotedMsg := contextInfo.QuotedMessage; quotedMsg != nil {
		quotedContent = extractTextContent(quotedMsg)
	}

	return quotedMessageId, quotedSender, quotedContent
}

// Extract media info from a message - uses provided timestamp for consistent filenames
func extractMediaInfo(msg *waProto.Message, msgTimestamp time.Time) (mediaType string, filename string, url string, mediaKey []byte, fileSHA256 []byte, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", nil, nil, nil, 0
	}

	// Use message timestamp for filename, fallback to current time if zero
	ts := msgTimestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	tsStr := ts.Format("20060102_150405")

	// Check for image message
	if img := msg.GetImageMessage(); img != nil {
		return "image", "image_" + tsStr + ".jpg",
			img.GetURL(), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}

	// Check for video message
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", "video_" + tsStr + ".mp4",
			vid.GetURL(), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}

	// Check for audio message
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + tsStr + ".ogg",
			aud.GetURL(), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}

	// Check for document message
	if doc := msg.GetDocumentMessage(); doc != nil {
		filename := doc.GetFileName()
		if filename == "" {
			filename = "document_" + tsStr
		}
		return "document", filename,
			doc.GetURL(), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return "", "", "", nil, nil, nil, 0
}

// resolveLIDChat resolves a LID-based chat JID to its phone-based equivalent
// so that incoming and outgoing messages are stored under the same chat entry.
// The senderAlt/recipientAlt fields carry the phone JID on live messages;
// for history sync these will be empty and the function falls back to the
// whatsmeow LID store (populated during live message handling).
func resolveLIDChat(client *whatsmeow.Client, chat, senderAlt, recipientAlt types.JID, isFromMe bool) types.JID {
	// hosted.lid is a LID namespace as well. Handling only HiddenUserServer here
	// while normalizeSender handles both meant a hosted-LID conversation could
	// normalise its sender to a phone number while chat_jid stayed "<id>@hosted.lid".
	if chat.Server == types.HostedLIDServer {
		chat = types.JID{User: chat.User, Device: chat.Device, Server: types.HiddenUserServer}
	}
	if chat.Server != types.HiddenUserServer {
		return chat
	}

	// For incoming DMs the phone JID is in SenderAlt;
	// for outgoing DMs it is in RecipientAlt.
	var alt types.JID
	if !isFromMe && !senderAlt.IsEmpty() && senderAlt.Server == types.DefaultUserServer {
		alt = senderAlt.ToNonAD()
	} else if isFromMe && !recipientAlt.IsEmpty() && recipientAlt.Server == types.DefaultUserServer {
		alt = recipientAlt.ToNonAD()
	}

	if !alt.IsEmpty() {
		fmt.Printf("Resolved LID chat %s -> %s (from message alt)\n", chat, alt)
		return alt
	}

	// Fallback: query the whatsmeow LID-PN mapping store.
	pn, err := client.Store.LIDs.GetPNForLID(context.Background(), chat)
	if err == nil && !pn.IsEmpty() {
		fmt.Printf("Resolved LID chat %s -> %s (from LID store)\n", chat, pn.ToNonAD())
		return pn.ToNonAD()
	}

	fmt.Printf("Warning: could not resolve LID chat %s to phone JID\n", chat)
	return chat
}

// recordOutgoing writes a message this process just sent into the message store.
//
// WhatsApp never echoes a message back to the device that sent it, and
// handleMessage is the only other writer — so without this, everything the
// bridge sends (digests, agent replies) leaves no trace in messages.db. Only
// messages sent from *another* device on the same account arrive as events and
// get stored, which is why the local own-send record went silent once the
// digest jobs moved onto this bridge's /api/send.
//
// It deliberately mirrors handleMessage: same content/media extraction, same
// chat-key normalisation (DMs are keyed by phone JID even though we send to the
// LID), so an own-send row is indistinguishable from an echoed one.
// phoneAltOf returns the phone JID for a recipient when one is known: itself if
// it is already a phone JID, or the mapped phone number if it is a LID. The
// /api/reply and /api/forward handlers send to whatever JID they were given, so
// unlike /api/send they have no pre-swap phone form to remember.
func phoneAltOf(client *whatsmeow.Client, jid types.JID) types.JID {
	if jid.Server == types.DefaultUserServer {
		return jid.ToNonAD()
	}
	if jid.Server == types.HiddenUserServer || jid.Server == types.HostedLIDServer {
		bare := jid.ToNonAD()
		bare.Server = types.HiddenUserServer
		if pn, err := client.Store.LIDs.GetPNForLID(context.Background(), bare); err == nil && !pn.IsEmpty() {
			return pn.ToNonAD()
		}
	}
	return types.JID{}
}

// recipientAlt is the phone JID the caller started from, when it had one. Sends
// resolve a phone number to a LID before dispatch, so we usually know the phone
// form already — and passing it here is what guarantees this row lands under the
// same chat key handleMessage would compute. Without it, resolution depends on
// the local LID map already being populated, and a miss files the same
// conversation under both "<lid>@lid" and "<phone>@s.whatsapp.net".
func recordOutgoing(client *whatsmeow.Client, messageStore *MessageStore, chat types.JID, recipientAlt types.JID, resp whatsmeow.SendResponse, msg *waProto.Message) {
	if messageStore == nil || msg == nil || resp.ID == "" {
		return
	}

	chatJID := resolveLIDChat(client, chat, types.JID{}, recipientAlt, true).String()

	// Match the sender identity the inbound path records for our own messages:
	// the LID user since the 2026-07-14 LID migration, the phone user before it.
	// Our own identity, normalised the same way every other sender is: phone
	// number canonical, LID as the alias.
	sender, senderLID := "", ""
	if own := client.Store.GetJID(); !own.IsEmpty() {
		sender, senderLID = normalizeSender(client, own)
	}
	if lid := client.Store.GetLID(); !lid.IsEmpty() {
		if senderLID == "" {
			senderLID = lid.ToNonAD().User
		}
		if sender == "" {
			sender = lid.ToNonAD().User + "@lid"
		}
	}

	content := extractTextContent(msg)
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg, resp.Timestamp)
	if content == "" && mediaType == "" {
		return
	}

	if err := messageStore.StoreMessage(
		resp.ID, chatJID, sender, senderLID, content, resp.Timestamp, true,
		mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	); err != nil {
		fmt.Printf("Warning: failed to record outgoing message %s in %s: %v\n", resp.ID, chatJID, err)
		return
	}
	if err := messageStore.TouchChat(chatJID, resp.Timestamp); err != nil {
		fmt.Printf("Warning: failed to bump chat time for %s: %v\n", chatJID, err)
	}

	timestamp := resp.Timestamp.Format("2006-01-02 15:04:05")
	if mediaType != "" {
		fmt.Printf("[%s] → %s: [%s: %s] %s\n", timestamp, chatJID, mediaType, filename, content)
	} else {
		fmt.Printf("[%s] → %s: %s\n", timestamp, chatJID, content)
	}
}

// Handle regular incoming messages with media support
func handleMessage(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, logger waLog.Logger) {
	// Resolve LID-based chats to phone-based JIDs so that incoming
	// and outgoing messages land in the same chat entry.
	resolvedChat := resolveLIDChat(client, msg.Info.Chat, msg.Info.SenderAlt, msg.Info.RecipientAlt, msg.Info.IsFromMe)
	chatJID := resolvedChat.String()

	// Prefer the phone JID the message already carries (SenderAlt on LID-addressed
	// messages) over a store lookup — it is authoritative and always present on
	// live messages. Fall back to the LID map for history sync, where it is empty.
	senderJID := msg.Info.Sender
	if (senderJID.Server == types.HiddenUserServer || senderJID.Server == types.HostedLIDServer) &&
		!msg.Info.SenderAlt.IsEmpty() && msg.Info.SenderAlt.Server == types.DefaultUserServer {
		senderJID = msg.Info.SenderAlt
	}
	sender, senderLID := normalizeSender(client, senderJID)
	if senderLID == "" && (msg.Info.Sender.Server == types.HiddenUserServer ||
		msg.Info.Sender.Server == types.HostedLIDServer) {
		senderLID = msg.Info.Sender.ToNonAD().User
	}

	// Get appropriate chat name (pass resolved JID so contact lookup works)
	name := GetChatName(client, messageStore, resolvedChat, chatJID, nil, sender, logger)

	// If contact resolution fails (common for LIDs), PushName is often the best available display name.
	// Only apply for direct messages (not groups) and only when the stored name is the numeric JID user.
	if !msg.Info.IsFromMe && msg.Info.Chat.Server != "g.us" && strings.TrimSpace(msg.Info.PushName) != "" {
		pushName := strings.TrimSpace(msg.Info.PushName)
		if name == "" || name == msg.Info.Chat.User {
			logger.Infof("Updating chat name from PushName for %s: %s -> %s", chatJID, name, pushName)
			name = pushName
		}
	}

	// Update chat in database with the message timestamp (keeps last message time updated)
	err := messageStore.StoreChat(chatJID, name, msg.Info.Timestamp)
	if err != nil {
		logger.Warnf("Failed to store chat: %v", err)
	}

	// Extract text content
	content := extractTextContent(msg.Message)

	// Extract media info - pass message timestamp for consistent filenames
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg.Message, msg.Info.Timestamp)

	// Extract quoted message info
	quotedMessageId, quotedSender, quotedContent := extractQuotedMessageInfo(msg.Message)

	// Skip if there's no content and no media
	if content == "" && mediaType == "" {
		return
	}

	// NOTE: a second download used to fire here, BEFORE StoreMessage. downloadMedia
	// re-reads the message from the DB, so it raced the insert it depends on and
	// lost most of the time — 471 "failed to find message: sql: no rows" against
	// 133 successes in the current log. Removed; autoDownloadMedia below is the
	// single download path and waits for the write to commit.

	// Store message in database
	err = messageStore.StoreMessage(
		msg.Info.ID,
		chatJID,
		sender,
		senderLID,
		content,
		msg.Info.Timestamp,
		msg.Info.IsFromMe,
		mediaType,
		filename,
		url,
		mediaKey,
		fileSHA256,
		fileEncSHA256,
		fileLength,
	)

	// Download every piece of media that arrives, unconditionally.
	//
	// This used to be gated on AUTO_DOWNLOAD_MEDIA=true and to skip own-sends.
	// Both were wrong in practice: the env var was never set on this box, so a
	// PDF dropped in a group (the murf.ai deck, 5 Aug) existed only as a row
	// with no file behind it, and every later "read that attachment" needed a
	// manual fetch that nobody did. KC's call 2026-08-06: pull everything.
	//
	// Set AUTO_DOWNLOAD_MEDIA=false to opt back out.
	// Only after a successful store: autoDownloadMedia re-reads the message from
	// the DB, so firing it when the write failed just guarantees a failed lookup.
	if err == nil && mediaType != "" && os.Getenv("AUTO_DOWNLOAD_MEDIA") != "false" {
		go autoDownloadMedia(client, messageStore, msg, chatJID, mediaType, filename, logger)
	}

	// Send webhook for incoming messages (text OR media — a voice note has
	// empty content, and the old content-only guard meant the listener never
	// heard about it at all). Forward self-messages when FORWARD_SELF=true.
	if (content != "" || mediaType != "") && (forwardSelfMessages || !msg.Info.IsFromMe) {
		isPTT := false
		if aud := msg.Message.GetAudioMessage(); aud != nil {
			isPTT = aud.GetPTT()
		}
		SendWebhook(msg.Info.ID, sender, content, chatJID, msg.Info.IsFromMe, quotedMessageId, quotedSender, quotedContent, mediaType, filename, fileLength, isPTT)
	}

	if err != nil {
		logger.Warnf("Failed to store message: %v", err)
	} else {
		// Log message reception
		timestamp := msg.Info.Timestamp.Format("2006-01-02 15:04:05")
		direction := "←"
		if msg.Info.IsFromMe {
			direction = "→"
		}

		// Log based on message type
		if mediaType != "" {
			fmt.Printf("[%s] %s %s: [%s: %s] %s\n", timestamp, direction, sender, mediaType, filename, content)
		} else if content != "" {
			fmt.Printf("[%s] %s %s: %s\n", timestamp, direction, sender, content)
		}
	}
}

// DownloadMediaRequest represents the request body for the download media API
type DownloadMediaRequest struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
}

// DownloadMediaResponse represents the response for the download media API
type DownloadMediaResponse struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	Filename string `json:"filename,omitempty"`
	Path     string `json:"path,omitempty"`
}

// Store additional media info in the database
func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		"UPDATE messages SET url = ?, media_key = ?, file_sha256 = ?, file_enc_sha256 = ?, file_length = ? WHERE id = ? AND chat_jid = ?",
		url, mediaKey, fileSHA256, fileEncSHA256, fileLength, id, chatJID,
	)
	return err
}

// Get media info from the database
func (store *MessageStore) GetMediaInfo(id, chatJID string) (string, string, string, []byte, []byte, []byte, uint64, error) {
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64

	err := store.db.QueryRow(
		"SELECT media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length FROM messages WHERE id = ? AND chat_jid = ?",
		id, chatJID,
	).Scan(&mediaType, &filename, &url, &mediaKey, &fileSHA256, &fileEncSHA256, &fileLength)

	return mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err
}

// MediaDownloader implements the whatsmeow.DownloadableMessage interface
type MediaDownloader struct {
	URL           string
	DirectPath    string
	MediaKey      []byte
	FileLength    uint64
	FileSHA256    []byte
	FileEncSHA256 []byte
	MediaType     whatsmeow.MediaType
}

// GetDirectPath implements the DownloadableMessage interface
func (d *MediaDownloader) GetDirectPath() string {
	return d.DirectPath
}

// GetURL implements the DownloadableMessage interface
func (d *MediaDownloader) GetURL() string {
	return d.URL
}

// GetMediaKey implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaKey() []byte {
	return d.MediaKey
}

// GetFileLength implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileLength() uint64 {
	return d.FileLength
}

// GetFileSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileSHA256() []byte {
	return d.FileSHA256
}

// GetFileEncSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileEncSHA256() []byte {
	return d.FileEncSHA256
}

// GetMediaType implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaType() whatsmeow.MediaType {
	return d.MediaType
}

// Per-message download locks: the event-handler goroutine and the listener's
// /api/download can request the same media concurrently; without this, both
// pass the existence check and race the write.
var mediaDownloadLocks sync.Map // "messageID|chatJID" -> *sync.Mutex

// sanitizeIDForFilename keeps a WhatsApp message ID filesystem-safe (IDs are
// uppercase hex in practice, but never trust an external ID in a path).
func sanitizeIDForFilename(id string) string {
	var b strings.Builder
	for _, r := range id {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "noid"
	}
	return b.String()
}

// Function to download media from a message
func downloadMedia(client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string) (bool, string, string, string, error) {
	// Query the database for the message including timestamp
	var mediaType, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64
	var timestamp time.Time
	var err error

	// Get media info AND timestamp from the database
	err = messageStore.db.QueryRow(
		"SELECT media_type, url, media_key, file_sha256, file_enc_sha256, file_length, timestamp FROM messages WHERE id = ? AND chat_jid = ?",
		messageID, chatJID,
	).Scan(&mediaType, &url, &mediaKey, &fileSHA256, &fileEncSHA256, &fileLength, &timestamp)

	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to find message: %v", err)
	}

	// Check if this is a media message
	if mediaType == "" {
		return false, "", "", "", fmt.Errorf("not a media message")
	}

	// Generate filename from message timestamp (not stored filename which may be wrong)
	var ext string
	switch mediaType {
	case "image":
		ext = ".jpg"
	case "video":
		ext = ".mp4"
	case "audio":
		ext = ".ogg"
	case "document":
		ext = ""
	default:
		ext = ""
	}
	// Message ID in the name: a second-resolution timestamp alone collides when
	// two media messages land in the same second, silently serving the WRONG
	// file to whoever asks second (e.g. transcribing voice note A as B).
	filename := fmt.Sprintf("%s_%s_%s%s", mediaType, timestamp.Format("20060102_150405"), sanitizeIDForFilename(messageID), ext)

	// Serialize per message: only one goroutine may check-then-download a given
	// media file at a time.
	lockAny, _ := mediaDownloadLocks.LoadOrStore(messageID+"|"+chatJID, &sync.Mutex{})
	lock := lockAny.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	// First, check if we already have this file
	chatDir := fmt.Sprintf("store/%s", strings.ReplaceAll(chatJID, ":", "_"))

	// Create directory for the chat if it doesn't exist
	if err := os.MkdirAll(chatDir, 0755); err != nil {
		return false, "", "", "", fmt.Errorf("failed to create chat directory: %v", err)
	}

	// Generate a local path for the file
	localPath := fmt.Sprintf("%s/%s", chatDir, filename)

	// Get absolute path
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to get absolute path: %v", err)
	}

	// Check if file already exists
	if _, err := os.Stat(localPath); err == nil {
		// File exists, return it
		fmt.Printf("📁 File already exists: %s\n", absPath)
		return true, mediaType, filename, absPath, nil
	}

	// If we don't have all the media info we need, we can't download
	if url == "" || len(mediaKey) == 0 || len(fileSHA256) == 0 || len(fileEncSHA256) == 0 || fileLength == 0 {
		return false, "", "", "", fmt.Errorf("incomplete media information for download")
	}

	fmt.Printf("Attempting to download media for message %s in chat %s...\n", messageID, chatJID)

	// Extract direct path from URL
	directPath := extractDirectPathFromURL(url)

	// Create a downloader that implements DownloadableMessage
	var waMediaType whatsmeow.MediaType
	switch mediaType {
	case "image":
		waMediaType = whatsmeow.MediaImage
	case "video":
		waMediaType = whatsmeow.MediaVideo
	case "audio":
		waMediaType = whatsmeow.MediaAudio
	case "document":
		waMediaType = whatsmeow.MediaDocument
	default:
		return false, "", "", "", fmt.Errorf("unsupported media type: %s", mediaType)
	}

	downloader := &MediaDownloader{
		URL:           url,
		DirectPath:    directPath,
		MediaKey:      mediaKey,
		FileLength:    fileLength,
		FileSHA256:    fileSHA256,
		FileEncSHA256: fileEncSHA256,
		MediaType:     waMediaType,
	}

	// Download the media using whatsmeow client
	mediaData, err := client.Download(context.Background(), downloader)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to download media: %v", err)
	}

	// Save atomically (unique tmp + rename): a reader must never observe a
	// half-written file, and concurrent writers must never share a tmp path.
	tmpFile, err := os.CreateTemp(chatDir, filename+".tmp-*")
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to create temp media file: %v", err)
	}
	tmpPath := tmpFile.Name()
	if _, err := tmpFile.Write(mediaData); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return false, "", "", "", fmt.Errorf("failed to save media file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return false, "", "", "", fmt.Errorf("failed to close media file: %v", err)
	}
	_ = os.Chmod(tmpPath, 0644)
	if err := os.Rename(tmpPath, localPath); err != nil {
		_ = os.Remove(tmpPath)
		return false, "", "", "", fmt.Errorf("failed to finalize media file: %v", err)
	}

	fmt.Printf("Successfully downloaded %s media to %s (%d bytes)\n", mediaType, absPath, len(mediaData))
	return true, mediaType, filename, absPath, nil
}

// autoDownloadMedia is fired in a goroutine for incoming messages with media
// when AUTO_DOWNLOAD_MEDIA=true. Thin wrapper around the existing
// downloadMedia helper used by /api/download.
func autoDownloadMedia(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, chatJID, mediaType, filename string, logger waLog.Logger) {
	// Small delay so the StoreMessage write commits first — otherwise
	// downloadMedia (which re-reads the message from DB) races.
	time.Sleep(500 * time.Millisecond)
	ok, _, _, path, err := downloadMedia(client, messageStore, msg.Info.ID, chatJID)
	if err != nil {
		logger.Warnf("auto-download failed for %s/%s: %v", chatJID, msg.Info.ID, err)
		return
	}
	if ok {
		logger.Infof("auto-downloaded %s (%s) → %s", mediaType, filename, path)
	}
}

// Extract direct path from a WhatsApp media URL
func extractDirectPathFromURL(url string) string {
	// The direct path is typically in the URL, we need to extract it
	// Example URL: https://mmg.whatsapp.net/v/t62.7118-24/13812002_698058036224062_3424455886509161511_n.enc?ccb=11-4&oh=...

	// Find the path part after the domain
	parts := strings.SplitN(url, ".net/", 2)
	if len(parts) < 2 {
		return url // Return original URL if parsing fails
	}

	pathPart := parts[1]

	// Keep the query string (?ccb=&oh=&oe=&_nc_sid=&mms3=true). WhatsApp's media
	// CDN now REQUIRES the signed oh/oe tokens on every download and returns 403
	// without them. whatsmeow rebuilds the URL as host + directPath + "&hash=...",
	// so leaving the query on directPath yields host+/path?...signed...&hash=...,
	// which the CDN accepts. Stripping it here caused 403 on all media (any age).

	// Create proper direct path format
	return "/" + pathPart
}

// Start a REST API server to expose the WhatsApp client functionality
func startRESTServer(client *whatsmeow.Client, messageStore *MessageStore, port int) {
	// Health check endpoint
	http.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		status := map[string]interface{}{
			"status":    "ok",
			"connected": client.IsConnected(),
			"timestamp": time.Now().Unix(),
		}
		if !client.IsConnected() {
			status["status"] = "disconnected"
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(status)
	})

	// Handler for sending messages
	http.HandleFunc("/api/send", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req SendMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.Recipient == "" {
			http.Error(w, "Recipient is required", http.StatusBadRequest)
			return
		}

		if req.Message == "" && req.MediaPath == "" {
			http.Error(w, "Message or media path is required", http.StatusBadRequest)
			return
		}

		fmt.Println("Received request to send message", req.Message, req.MediaPath)

		// Send the message
		success, message := sendWhatsAppMessage(client, messageStore, req.Recipient, req.Message, req.MediaPath)
		fmt.Println("Message sent", success, message)
		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Set appropriate status code
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}

		// Send response
		_ = json.NewEncoder(w).Encode(SendMessageResponse{
			Success: success,
			Message: message,
		})
	})

	// Handler for downloading media
	http.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Check if connected
		if !client.IsConnected() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(DownloadMediaResponse{
				Success: false,
				Message: "WhatsApp client is not connected. Please wait for reconnection.",
			})
			return
		}

		// Parse the request body
		var req DownloadMediaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.MessageID == "" || req.ChatJID == "" {
			http.Error(w, "Message ID and Chat JID are required", http.StatusBadRequest)
			return
		}

		// Log download request for debugging
		fmt.Printf("📥 Download request: message_id=%s chat_jid=%s\n", req.MessageID, req.ChatJID)

		// Download the media
		success, mediaType, filename, path, err := downloadMedia(client, messageStore, req.MessageID, req.ChatJID)

		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Handle download result
		if !success || err != nil {
			errMsg := "Unknown error"
			if err != nil {
				errMsg = err.Error()
			}

			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(DownloadMediaResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to download media: %s", errMsg),
			})
			return
		}

		// Send successful response
		_ = json.NewEncoder(w).Encode(DownloadMediaResponse{
			Success:  true,
			Message:  fmt.Sprintf("Successfully downloaded %s media", mediaType),
			Filename: filename,
			Path:     path,
		})
	})

	// Handler for sending typing indicator
	http.HandleFunc("/api/typing", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req struct {
			Recipient string `json:"recipient"`
			IsTyping  bool   `json:"is_typing"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.Recipient == "" {
			http.Error(w, "Recipient is required", http.StatusBadRequest)
			return
		}

		// Create JID for recipient
		var recipientJID types.JID
		var err error

		// Check if recipient is a JID
		if strings.Contains(req.Recipient, "@") {
			recipientJID, err = types.ParseJID(req.Recipient)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"success": false,
					"message": fmt.Sprintf("Error parsing JID: %v", err),
				})
				return
			}
		} else {
			// Create JID from phone number
			recipientJID = types.JID{
				User:   req.Recipient,
				Server: "s.whatsapp.net",
			}
		}

		// Determine the chat presence state
		var state types.ChatPresence
		if req.IsTyping {
			state = types.ChatPresenceComposing
		} else {
			state = types.ChatPresencePaused
		}

		// Send the chat presence update
		err = client.SendChatPresence(context.Background(), recipientJID, state, types.ChatPresenceMediaText)

		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Send response
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": fmt.Sprintf("Failed to send typing indicator: %v", err),
			})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"message": fmt.Sprintf("Typing indicator set to %v", req.IsTyping),
			})
		}
	})

	// Helper: parse phone-or-JID into types.JID
	parseRecipient := func(s string) (types.JID, error) {
		if strings.Contains(s, "@") {
			return types.ParseJID(s)
		}
		return types.JID{User: s, Server: "s.whatsapp.net"}, nil
	}
	jsonOK := func(w http.ResponseWriter, message string) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "message": message})
	}
	jsonFail := func(w http.ResponseWriter, code int, msg string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": msg})
	}
	requirePost := func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return false
		}
		return true
	}

	// /api/reply — send a text message that quotes a parent message
	// /api/history_sync — ask the WhatsApp server for older messages in a
	// chat. Uses whatsmeow's BuildHistorySyncRequest + a peer-message to
	// ownID. Response arrives async as *events.HistorySync and lands in the
	// local DB via the existing handleHistorySync path.
	http.HandleFunc("/api/history_sync", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) { return }
		var req struct {
			ChatJID string `json:"chat_jid"`
			Count   int    `json:"count"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			jsonFail(w, 400, "invalid request"); return
		}
		if req.ChatJID == "" { jsonFail(w, 400, "chat_jid required"); return }
		if req.Count <= 0 { req.Count = 50 }
		if req.Count > 500 { req.Count = 500 }

		chatJID, err := types.ParseJID(req.ChatJID)
		if err != nil { jsonFail(w, 400, fmt.Sprintf("bad chat_jid: %v", err)); return }

		oldestID, oldestSender, oldestFromMe, oldestTS, err := messageStore.GetOldestMessage(req.ChatJID)
		if err != nil {
			jsonFail(w, 404, "no known messages in that chat to anchor history sync from"); return
		}

		var senderJID types.JID
		if strings.Contains(oldestSender, "@") {
			senderJID, _ = types.ParseJID(oldestSender)
		} else if oldestSender != "" {
			senderJID = types.JID{User: oldestSender, Server: types.DefaultUserServer}
		} else {
			senderJID = chatJID
		}

		info := &types.MessageInfo{
			ID:        oldestID,
			Timestamp: oldestTS,
			MessageSource: types.MessageSource{
				Chat:     chatJID,
				Sender:   senderJID,
				IsFromMe: oldestFromMe,
				IsGroup:  chatJID.Server == "g.us",
			},
		}
		msg := client.BuildHistorySyncRequest(info, req.Count)

		ownID := client.Store.ID
		if ownID == nil { jsonFail(w, 503, "client not logged in"); return }
		_, err = client.SendMessage(context.Background(), *ownID, msg, whatsmeow.SendRequestExtra{Peer: true})
		if err != nil {
			jsonFail(w, 500, fmt.Sprintf("history sync request failed: %v", err)); return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":           true,
			"message":           fmt.Sprintf("requested %d older messages for %s", req.Count, req.ChatJID),
			"anchor_message_id": oldestID,
			"anchor_timestamp":  oldestTS.Format(time.RFC3339),
		})
	})

	http.HandleFunc("/api/reply", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) { return }
		var req struct {
			Recipient        string `json:"recipient"`
			Message          string `json:"message"`
			ReplyToMessageID string `json:"reply_to_message_id"`
			ReplyToSender    string `json:"reply_to_sender"` // JID of original sender
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { jsonFail(w, 400, "invalid request"); return }
		if req.Recipient == "" || req.Message == "" || req.ReplyToMessageID == "" {
			jsonFail(w, 400, "recipient, message, reply_to_message_id required"); return
		}
		recipientJID, err := parseRecipient(req.Recipient)
		if err != nil { jsonFail(w, 400, fmt.Sprintf("bad recipient: %v", err)); return }
		quotedSender := req.ReplyToSender
		if quotedSender == "" { quotedSender = recipientJID.String() }
		mentions := extractMentions(req.Message)
		ctx := &waProto.ContextInfo{
			StanzaID:      proto.String(req.ReplyToMessageID),
			Participant:   proto.String(quotedSender),
			QuotedMessage: &waProto.Message{Conversation: proto.String("")},
			MentionedJID:  mentions, // tags render only if JID is also in this array
		}
		msg := &waProto.Message{ExtendedTextMessage: &waProto.ExtendedTextMessage{
			Text:        proto.String(req.Message),
			ContextInfo: ctx,
		}}
		resp, err := client.SendMessage(context.Background(), recipientJID, msg)
		if err != nil {
			jsonFail(w, 500, fmt.Sprintf("send failed: %v", err)); return
		}
		recordOutgoing(client, messageStore, recipientJID, phoneAltOf(client, recipientJID), resp, msg)
		// Return the message ID so the caller can later /api/edit or /api/revoke it.
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    true,
			"message":    "Reply sent",
			"message_id": resp.ID,
		})
	})

	// /api/forward — forward an existing text message to another chat
	// (For media, use download_media + send_file from the MCP side.)
	http.HandleFunc("/api/forward", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) { return }
		var req struct {
			Recipient      string `json:"recipient"`
			MessageID      string `json:"message_id"`
			SourceChatJID  string `json:"source_chat_jid"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { jsonFail(w, 400, "invalid request"); return }
		if req.Recipient == "" || req.MessageID == "" || req.SourceChatJID == "" {
			jsonFail(w, 400, "recipient, message_id, source_chat_jid required"); return
		}
		var content, mediaType string
		err := messageStore.db.QueryRow("SELECT content, media_type FROM messages WHERE id = ? AND chat_jid = ?", req.MessageID, req.SourceChatJID).Scan(&content, &mediaType)
		if err != nil { jsonFail(w, 404, fmt.Sprintf("source message not found: %v", err)); return }
		if mediaType != "" {
			jsonFail(w, 400, "media forward not supported; use download_media + send_file")
			return
		}
		recipientJID, err := parseRecipient(req.Recipient)
		if err != nil { jsonFail(w, 400, fmt.Sprintf("bad recipient: %v", err)); return }
		// Mark as forwarded so WhatsApp shows the "Forwarded" badge
		ctx := &waProto.ContextInfo{
			IsForwarded:     proto.Bool(true),
			ForwardingScore: proto.Uint32(1),
		}
		msg := &waProto.Message{ExtendedTextMessage: &waProto.ExtendedTextMessage{
			Text:        proto.String(content),
			ContextInfo: ctx,
		}}
		resp, err := client.SendMessage(context.Background(), recipientJID, msg)
		if err != nil {
			jsonFail(w, 500, fmt.Sprintf("send failed: %v", err)); return
		}
		recordOutgoing(client, messageStore, recipientJID, phoneAltOf(client, recipientJID), resp, msg)
		jsonOK(w, "Forwarded")
	})

	// /api/edit — edit a previously-sent message (15-min window)
	http.HandleFunc("/api/edit", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) { return }
		var req struct {
			Recipient  string `json:"recipient"`
			MessageID  string `json:"message_id"`
			NewMessage string `json:"new_message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { jsonFail(w, 400, "invalid request"); return }
		if req.Recipient == "" || req.MessageID == "" || req.NewMessage == "" {
			jsonFail(w, 400, "recipient, message_id, new_message required"); return
		}
		recipientJID, err := parseRecipient(req.Recipient)
		if err != nil { jsonFail(w, 400, fmt.Sprintf("bad recipient: %v", err)); return }
		newContent := &waProto.Message{Conversation: proto.String(req.NewMessage)}
		editMsg := client.BuildEdit(recipientJID, req.MessageID, newContent)
		if _, err := client.SendMessage(context.Background(), recipientJID, editMsg); err != nil {
			jsonFail(w, 500, fmt.Sprintf("edit failed: %v", err)); return
		}
		jsonOK(w, "Message edited")
	})

	// /api/revoke — delete a message for everyone (~2-day window)
	http.HandleFunc("/api/revoke", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) { return }
		var req struct {
			Recipient string `json:"recipient"`
			MessageID string `json:"message_id"`
			Sender    string `json:"sender"` // optional; defaults to self
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { jsonFail(w, 400, "invalid request"); return }
		if req.Recipient == "" || req.MessageID == "" {
			jsonFail(w, 400, "recipient, message_id required"); return
		}
		chatJID, err := parseRecipient(req.Recipient)
		if err != nil { jsonFail(w, 400, fmt.Sprintf("bad recipient: %v", err)); return }
		var senderJID types.JID
		if req.Sender != "" {
			senderJID, err = parseRecipient(req.Sender)
			if err != nil { jsonFail(w, 400, fmt.Sprintf("bad sender: %v", err)); return }
		} else {
			senderJID = *client.Store.ID
		}
		revokeMsg := client.BuildRevoke(chatJID, senderJID, req.MessageID)
		if _, err := client.SendMessage(context.Background(), chatJID, revokeMsg); err != nil {
			jsonFail(w, 500, fmt.Sprintf("revoke failed: %v", err)); return
		}
		jsonOK(w, "Message revoked")
	})

	// /api/read — send read (or delivered) receipt for one or more messages in a chat
	http.HandleFunc("/api/read", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) { return }
		var req struct {
			ChatJID     string   `json:"chat_jid"`
			SenderJID   string   `json:"sender_jid"`   // original message sender; required for groups
			MessageIDs  []string `json:"message_ids"`
			ReceiptType string   `json:"receipt_type"` // "read" (default), "delivered", "played"
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { jsonFail(w, 400, "invalid request"); return }
		if req.ChatJID == "" || len(req.MessageIDs) == 0 {
			jsonFail(w, 400, "chat_jid and message_ids required"); return
		}
		chatJID, err := types.ParseJID(req.ChatJID)
		if err != nil { jsonFail(w, 400, fmt.Sprintf("bad chat_jid: %v", err)); return }
		senderJID := chatJID
		if req.SenderJID != "" {
			senderJID, err = types.ParseJID(req.SenderJID)
			if err != nil { jsonFail(w, 400, fmt.Sprintf("bad sender_jid: %v", err)); return }
		}
		ids := make([]types.MessageID, len(req.MessageIDs))
		for i, id := range req.MessageIDs { ids[i] = types.MessageID(id) }
		var extra []types.ReceiptType
		switch strings.ToLower(req.ReceiptType) {
		case "delivered":
			extra = append(extra, types.ReceiptTypeDelivered)
		case "played":
			extra = append(extra, types.ReceiptTypePlayed)
		}
		if err := client.MarkRead(context.Background(), ids, time.Now(), chatJID, senderJID, extra...); err != nil {
			jsonFail(w, 500, fmt.Sprintf("MarkRead failed: %v", err)); return
		}
		jsonOK(w, fmt.Sprintf("Receipt sent for %d message(s)", len(ids)))
	})

	// /api/chat_read — mark all unread messages in a chat as read (batch helper)
	http.HandleFunc("/api/chat_read", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) { return }
		var req struct { ChatJID string `json:"chat_jid"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { jsonFail(w, 400, "invalid request"); return }
		if req.ChatJID == "" { jsonFail(w, 400, "chat_jid required"); return }
		chatJID, err := types.ParseJID(req.ChatJID)
		if err != nil { jsonFail(w, 400, fmt.Sprintf("bad chat_jid: %v", err)); return }
		// Group by sender and batch MarkRead per (chat, sender) tuple
		rows, err := messageStore.db.Query(
			"SELECT id, sender FROM messages WHERE chat_jid = ? AND is_from_me = 0 ORDER BY timestamp DESC LIMIT 200",
			req.ChatJID,
		)
		if err != nil { jsonFail(w, 500, fmt.Sprintf("db query failed: %v", err)); return }
		defer func() { _ = rows.Close() }()
		bySender := make(map[string][]types.MessageID)
		for rows.Next() {
			var id, sender string
			if err := rows.Scan(&id, &sender); err != nil { continue }
			if sender == "" { sender = req.ChatJID }
			bySender[sender] = append(bySender[sender], types.MessageID(id))
		}
		marked := 0
		for senderStr, ids := range bySender {
			senderJID, err := types.ParseJID(senderStr)
			if err != nil { continue }
			if err := client.MarkRead(context.Background(), ids, time.Now(), chatJID, senderJID); err == nil {
				marked += len(ids)
			}
		}
		jsonOK(w, fmt.Sprintf("Marked %d message(s) as read", marked))
	})

	// /api/group/info — get metadata and participants for a group JID
	http.HandleFunc("/api/group/info", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) { return }
		var req struct { GroupJID string `json:"group_jid"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { jsonFail(w, 400, "invalid request"); return }
		if req.GroupJID == "" { jsonFail(w, 400, "group_jid required"); return }
		groupJID, err := types.ParseJID(req.GroupJID)
		if err != nil { jsonFail(w, 400, fmt.Sprintf("bad group_jid: %v", err)); return }
		info, err := client.GetGroupInfo(context.Background(), groupJID)
		if err != nil { jsonFail(w, 500, fmt.Sprintf("GetGroupInfo failed: %v", err)); return }
		participants := make([]map[string]interface{}, 0, len(info.Participants))
		for _, p := range info.Participants {
			participants = append(participants, map[string]interface{}{
				"jid":          p.JID.String(),
				"lid":          p.LID.String(),
				"display_name": p.DisplayName,
				"is_admin":     p.IsAdmin,
				"is_super_admin": p.IsSuperAdmin,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":      true,
			"jid":          info.JID.String(),
			"name":         info.GroupName.Name,
			"topic":        info.GroupTopic.Topic,
			"created":      info.GroupCreated,
			"owner":        info.OwnerJID.String(),
			"is_locked":    info.GroupLocked.IsLocked,
			"is_announce":  info.GroupAnnounce.IsAnnounce,
			"participants": participants,
		})
	})

	// /api/group/participants — add / remove / promote / demote
	http.HandleFunc("/api/group/participants", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) { return }
		var req struct {
			GroupJID string   `json:"group_jid"`
			Action   string   `json:"action"` // "add", "remove", "promote", "demote"
			JIDs     []string `json:"jids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { jsonFail(w, 400, "invalid request"); return }
		if req.GroupJID == "" || req.Action == "" || len(req.JIDs) == 0 {
			jsonFail(w, 400, "group_jid, action, jids required"); return
		}
		groupJID, err := types.ParseJID(req.GroupJID)
		if err != nil { jsonFail(w, 400, fmt.Sprintf("bad group_jid: %v", err)); return }
		var change whatsmeow.ParticipantChange
		switch strings.ToLower(req.Action) {
		case "add":     change = whatsmeow.ParticipantChangeAdd
		case "remove":  change = whatsmeow.ParticipantChangeRemove
		case "promote": change = whatsmeow.ParticipantChangePromote
		case "demote":  change = whatsmeow.ParticipantChangeDemote
		default: jsonFail(w, 400, "action must be add|remove|promote|demote"); return
		}
		jids := make([]types.JID, 0, len(req.JIDs))
		for _, s := range req.JIDs {
			j, err := parseRecipient(s)
			if err != nil { jsonFail(w, 400, fmt.Sprintf("bad jid %s: %v", s, err)); return }
			jids = append(jids, j)
		}
		results, err := client.UpdateGroupParticipants(context.Background(), groupJID, jids, change)
		if err != nil { jsonFail(w, 500, fmt.Sprintf("UpdateGroupParticipants failed: %v", err)); return }
		out := make([]map[string]interface{}, 0, len(results))
		for _, p := range results {
			out = append(out, map[string]interface{}{
				"jid":      p.JID.String(),
				"is_admin": p.IsAdmin,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":      true,
			"message":      fmt.Sprintf("%s applied to %d participant(s)", req.Action, len(results)),
			"participants": out,
		})
	})

	// /api/location — send a static location share
	http.HandleFunc("/api/location", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) { return }
		var req struct {
			Recipient string  `json:"recipient"`
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
			Name      string  `json:"name"`
			Address   string  `json:"address"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { jsonFail(w, 400, "invalid request"); return }
		if req.Recipient == "" { jsonFail(w, 400, "recipient required"); return }
		recipientJID, err := parseRecipient(req.Recipient)
		if err != nil { jsonFail(w, 400, fmt.Sprintf("bad recipient: %v", err)); return }
		msg := &waProto.Message{LocationMessage: &waProto.LocationMessage{
			DegreesLatitude:  proto.Float64(req.Latitude),
			DegreesLongitude: proto.Float64(req.Longitude),
			Name:             proto.String(req.Name),
			Address:          proto.String(req.Address),
		}}
		if _, err := client.SendMessage(context.Background(), recipientJID, msg); err != nil {
			jsonFail(w, 500, fmt.Sprintf("send failed: %v", err)); return
		}
		jsonOK(w, "Location sent")
	})

	// /api/sticker — send a .webp sticker
	http.HandleFunc("/api/sticker", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) { return }
		var req struct {
			Recipient   string `json:"recipient"`
			StickerPath string `json:"sticker_path"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { jsonFail(w, 400, "invalid request"); return }
		if req.Recipient == "" || req.StickerPath == "" { jsonFail(w, 400, "recipient and sticker_path required"); return }
		if !strings.HasSuffix(strings.ToLower(req.StickerPath), ".webp") {
			jsonFail(w, 400, "sticker must be a .webp file"); return
		}
		recipientJID, err := parseRecipient(req.Recipient)
		if err != nil { jsonFail(w, 400, fmt.Sprintf("bad recipient: %v", err)); return }
		data, err := os.ReadFile(req.StickerPath)
		if err != nil { jsonFail(w, 400, fmt.Sprintf("read sticker: %v", err)); return }
		resp, err := client.Upload(context.Background(), data, whatsmeow.MediaImage)
		if err != nil { jsonFail(w, 500, fmt.Sprintf("upload failed: %v", err)); return }
		msg := &waProto.Message{StickerMessage: &waProto.StickerMessage{
			URL:           &resp.URL,
			DirectPath:    &resp.DirectPath,
			MediaKey:      resp.MediaKey,
			Mimetype:      proto.String("image/webp"),
			FileEncSHA256: resp.FileEncSHA256,
			FileSHA256:    resp.FileSHA256,
			FileLength:    &resp.FileLength,
		}}
		if _, err := client.SendMessage(context.Background(), recipientJID, msg); err != nil {
			jsonFail(w, 500, fmt.Sprintf("send failed: %v", err)); return
		}
		jsonOK(w, "Sticker sent")
	})

	// /api/calls/recent — list recent incoming-call events from the local DB
	http.HandleFunc("/api/calls/recent", func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) { return }
		var req struct {
			Limit int    `json:"limit"`
			After string `json:"after"` // ISO timestamp
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Limit <= 0 || req.Limit > 500 { req.Limit = 50 }
		var rows *sql.Rows
		var err error
		if req.After != "" {
			rows, err = messageStore.db.Query(
				"SELECT call_id, caller_jid, from_jid, event_type, media, is_group, timestamp FROM calls WHERE timestamp > ? ORDER BY timestamp DESC LIMIT ?",
				req.After, req.Limit,
			)
		} else {
			rows, err = messageStore.db.Query(
				"SELECT call_id, caller_jid, from_jid, event_type, media, is_group, timestamp FROM calls ORDER BY timestamp DESC LIMIT ?",
				req.Limit,
			)
		}
		if err != nil { jsonFail(w, 500, fmt.Sprintf("db query failed: %v", err)); return }
		defer func() { _ = rows.Close() }()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var callID, callerJID, fromJID, eventType, media string
			var isGroup bool
			var ts time.Time
			if err := rows.Scan(&callID, &callerJID, &fromJID, &eventType, &media, &isGroup, &ts); err == nil {
				out = append(out, map[string]interface{}{
					"call_id":    callID,
					"caller_jid": callerJID,
					"from_jid":   fromJID,
					"event_type": eventType,
					"media":      media,
					"is_group":   isGroup,
					"timestamp":  ts.Format(time.RFC3339),
				})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "calls": out})
	})

	// Handler for sending an emoji reaction to an existing message
	http.HandleFunc("/api/react", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Recipient string `json:"recipient"`
			MessageID string `json:"message_id"`
			Emoji     string `json:"emoji"`
			FromMe    bool   `json:"from_me"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		if req.Recipient == "" || req.MessageID == "" || req.Emoji == "" {
			http.Error(w, "recipient, message_id, and emoji are required", http.StatusBadRequest)
			return
		}

		var recipientJID types.JID
		var err error
		if strings.Contains(req.Recipient, "@") {
			recipientJID, err = types.ParseJID(req.Recipient)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"success": false,
					"message": fmt.Sprintf("Error parsing JID: %v", err),
				})
				return
			}
		} else {
			recipientJID = types.JID{User: req.Recipient, Server: "s.whatsapp.net"}
		}

		remoteJIDStr := recipientJID.String()
		fromMe := req.FromMe
		ts := time.Now().UnixMilli()

		msg := &waProto.Message{
			ReactionMessage: &waProto.ReactionMessage{
				Key: &waProto.MessageKey{
					RemoteJID: &remoteJIDStr,
					FromMe:    &fromMe,
					ID:        &req.MessageID,
				},
				Text:              &req.Emoji,
				SenderTimestampMS: &ts,
			},
		}

		_, sendErr := client.SendMessage(context.Background(), recipientJID, msg)
		w.Header().Set("Content-Type", "application/json")
		if sendErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": fmt.Sprintf("Error sending reaction: %v", sendErr),
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": fmt.Sprintf("Reaction %s sent to %s on message %s", req.Emoji, req.Recipient, req.MessageID),
		})
	})

	// Start the server with proper timeouts
	serverAddr := fmt.Sprintf(":%d", port)
	fmt.Printf("Starting REST API server on %s...\n", serverAddr)

	// Create server with timeouts for stability
	server := &http.Server{
		Addr:         serverAddr,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second, // Longer for media downloads
		IdleTimeout:  120 * time.Second,
	}

	// Run server in a goroutine so it doesn't block
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("REST API server error: %v\n", err)
		}
	}()
}

func main() {
	// Set up logger with DEBUG level for more detailed logging
	logger := waLog.Stdout("Client", "DEBUG", true)
	logger.Infof("Starting WhatsApp client...")

	if forwardSelfMessages {
		logger.Infof("FORWARD_SELF enabled: forwarding self messages to webhook")
	} else {
		logger.Infof("FORWARD_SELF disabled: self messages will NOT be forwarded")
	}

	// Create database connection for storing session data
	dbLog := waLog.Stdout("Database", "INFO", true)

	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		logger.Errorf("Failed to create store directory: %v", err)
		return
	}

	container, err := sqlstore.New(context.Background(), "sqlite3", "file:store/whatsapp.db?_foreign_keys=on", dbLog)
	if err != nil {
		logger.Errorf("Failed to connect to database: %v", err)
		return
	}

	// Get device store - This contains session information
	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		if err == sql.ErrNoRows {
			// No device exists, create one
			deviceStore = container.NewDevice()
			logger.Infof("Created new device")
		} else {
			logger.Errorf("Failed to get device: %v", err)
			return
		}
	}

	// Create client instance
	client := whatsmeow.NewClient(deviceStore, logger)
	if client == nil {
		logger.Errorf("Failed to create WhatsApp client")
		return
	}

	// Initialize message store
	messageStore, err := NewMessageStore()
	if err != nil {
		logger.Errorf("Failed to initialize message store: %v", err)
		return
	}
	defer func() { _ = messageStore.Close() }()

	if err := messageStore.MigrateLegacyLIDChatsToPhoneJIDs("store/whatsapp.db", logger); err != nil {
		logger.Errorf("Failed to migrate legacy LID chat rows: %v", err)
		return
	}

	if err := messageStore.NormalizeExistingSenders("store/whatsapp.db", logger); err != nil {
		logger.Errorf("Failed to normalise historical senders: %v", err)
		return
	}

	// Channel to signal reconnection needs
	reconnectChan := make(chan bool, 1)

	// Setup event handling for messages and history sync
	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			// Process regular messages
			handleMessage(client, messageStore, v, logger)

		case *events.HistorySync:
			// Process history sync events
			handleHistorySync(client, messageStore, v, logger)

		case *events.GroupInfo:
			// A chat name is written once and then cached forever, so a renamed
			// group keeps its old subject indefinitely — the AD working group read
			// as "AD - KR - RR - KC" for days after it became "AD - KR - RR - KC - AK".
			// Renames arrive as an event; take them.
			if v.Name != nil && v.Name.Name != "" {
				if err := messageStore.RenameChat(v.JID.String(), v.Name.Name); err != nil {
					logger.Warnf("Failed to rename chat %s: %v", v.JID, err)
				} else {
					logger.Infof("Group renamed: %s → %s", v.JID, v.Name.Name)
				}
			}

		case *events.Connected:
			logger.Infof("✓ Successfully connected to WhatsApp servers")

		case *events.LoggedOut:
			logger.Warnf("⚠️  Device logged out, please scan QR code to log in again")

		case *events.Disconnected:
			logger.Warnf("⚠️  Disconnected from WhatsApp servers, will attempt reconnection...")
			// Signal reconnection needed
			select {
			case reconnectChan <- true:
			default:
				// Channel already has a reconnect signal
			}

		case *events.ConnectFailure:
			logger.Errorf("❌ Connection failure: %v", v.Reason)
			// Signal reconnection needed
			select {
			case reconnectChan <- true:
			default:
			}

		case *events.StreamError:
			logger.Errorf("❌ Stream error: %v", v.Code)
			// Signal reconnection needed
			select {
			case reconnectChan <- true:
			default:
			}

		case *events.ClientOutdated:
			logger.Errorf("❌ Client outdated - please update whatsmeow library")

		case *events.CallOffer:
			logCallEvent(messageStore, v.CallID, v.CallCreator.String(), v.From.String(), "offer", "", false)
		case *events.CallOfferNotice:
			isGroup := v.Type == "group"
			logCallEvent(messageStore, v.CallID, v.CallCreator.String(), v.CallCreator.String(), "offer_notice", v.Media, isGroup)
		case *events.CallAccept:
			logCallEvent(messageStore, v.CallID, v.CallCreator.String(), v.From.String(), "accept", "", false)
		case *events.CallTerminate:
			logCallEvent(messageStore, v.CallID, v.CallCreator.String(), v.From.String(), "terminate:"+v.Reason, "", false)
		case *events.CallReject:
			logCallEvent(messageStore, v.CallID, v.CallCreator.String(), v.From.String(), "reject", "", false)
		}
	})

	// Create channel to track connection success
	connected := make(chan bool, 1)

	// Add connection retry logic
	maxRetries := 3
	var connErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		logger.Infof("Connection attempt %d/%d...", attempt, maxRetries)

		// Connect to WhatsApp
		if client.Store.ID == nil {
			// No ID stored, this is a new client, need to pair with phone
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			qrChan, connErr := client.GetQRChannel(ctx)
			if connErr != nil {
				logger.Errorf("Failed to get QR channel: %v", connErr)
				if attempt == maxRetries {
					return
				}
				time.Sleep(5 * time.Second)
				continue
			}

			connErr = client.Connect()
			if connErr != nil {
				logger.Errorf("Failed to connect (attempt %d): %v", attempt, connErr)
				if attempt == maxRetries {
					return
				}
				time.Sleep(5 * time.Second)
				continue
			}

			// Print QR code for pairing with phone
			qrCodeShown := false
			for evt := range qrChan {
				if evt.Event == "code" {
					if !qrCodeShown {
						fmt.Println("\nScan this QR code with your WhatsApp app:")
						qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
						fmt.Println("\nWaiting for QR code scan...")
						qrCodeShown = true
					}
				} else if evt.Event == "success" {
					connected <- true
					break
				} else if evt.Event == "timeout" {
					logger.Warnf("QR code timed out")
					break
				}
			}

			// Wait for connection with timeout
			select {
			case <-connected:
				fmt.Println("\nSuccessfully connected and authenticated!")
				goto connectionSuccess
			case <-ctx.Done():
				logger.Errorf("Timeout waiting for QR code scan (attempt %d)", attempt)
				client.Disconnect()
				if attempt == maxRetries {
					return
				}
				time.Sleep(10 * time.Second)
				continue
			}
		} else {
			// Already logged in, just connect
			connErr = client.Connect()
			if connErr != nil {
				logger.Errorf("Failed to connect (attempt %d): %v", attempt, connErr)
				if attempt == maxRetries {
					return
				}
				time.Sleep(5 * time.Second)
				continue
			}
			connected <- true
			break
		}
	}

connectionSuccess:

	// Wait a moment for connection to stabilize
	time.Sleep(2 * time.Second)

	if !client.IsConnected() {
		logger.Errorf("Failed to establish stable connection")
		return
	}

	fmt.Println("\n✓ Connected to WhatsApp! Type 'help' for commands.")

	// Start REST API server
	port := 8080
	if p := os.Getenv("WHATSAPP_BRIDGE_PORT"); p != "" {
		v, err := strconv.Atoi(p)
		if err != nil || v < 1 || v > 65535 {
			logger.Errorf("Invalid WHATSAPP_BRIDGE_PORT=%q, must be 1-65535", p)
			return
		}
		port = v
	}
	startRESTServer(client, messageStore, port)

	// Create a channel to keep the main goroutine alive
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("REST server is running. Press Ctrl+C to disconnect and exit.")

	// Start reconnection handler goroutine
	go func() {
		reconnectBackoff := time.Second * 5
		maxBackoff := time.Minute * 5

		for {
			select {
			case <-reconnectChan:
				logger.Infof("🔄 Attempting to reconnect...")

				// Wait before reconnecting
				time.Sleep(reconnectBackoff)

				// Try to reconnect
				if !client.IsConnected() {
					err := client.Connect()
					if err != nil {
						logger.Errorf("❌ Reconnection failed: %v", err)
						// Increase backoff for next attempt
						reconnectBackoff = reconnectBackoff * 2
						if reconnectBackoff > maxBackoff {
							reconnectBackoff = maxBackoff
						}
						// Signal another reconnection attempt
						select {
						case reconnectChan <- true:
						default:
						}
					} else {
						logger.Infof("✓ Reconnected successfully")
						// Reset backoff on successful connection
						reconnectBackoff = time.Second * 5
					}
				} else {
					logger.Infof("Already connected, skipping reconnection")
					reconnectBackoff = time.Second * 5
				}

			case <-exitChan:
				return
			}
		}
	}()

	// Wait for termination signal
	<-exitChan

	fmt.Println("Disconnecting...")
	// Disconnect client
	client.Disconnect()
}

// GetChatName determines the appropriate name for a chat based on JID and other info
// normalizeSender converts a message's sender JID into the pair we store:
// a canonical identity and its LID alias.
//
// WhatsApp is migrating to LID addressing — this account migrated 2026-07-14 —
// so the same person arrives as a phone number before the switch and as a LID
// after it. The sender column had accumulated three spellings as a result
// (bare digits that could be either, and LIDs with an "@lid" suffix), which
// meant `147609603813461` and `147609603813461@lid` were the same person and
// no query matched both.
//
// The phone number is the canonical form because it is the only identifier that
// joins WhatsApp to email, Slack and the contact book. A LID means nothing
// outside WhatsApp.
//
// Returns (sender, senderLID):
//   - resolvable LID  → ("919499499994", "147609603813461")
//   - phone JID       → ("919499499994", "147609603813461") when the reverse
//     mapping is known, else ("919499499994", "")
//   - unresolvable LID → ("147609603813461@lid", "147609603813461") — the suffix
//     is deliberate, so an unresolved LID can never be misread as a phone number.
//     That confusion is what caused a whole session to mistake KC for his own bot.
func normalizeSender(client *whatsmeow.Client, sender types.JID) (string, string) {
	if sender.IsEmpty() {
		return "", ""
	}
	ctx := context.Background()

	// Only person-shaped namespaces carry an identity we can normalise. A group,
	// broadcast or newsletter JID reaching here would otherwise be stripped to a
	// bare user and could collide with a real phone number — "status@broadcast"
	// becoming the sender "status". Keep those whole.
	switch sender.Server {
	case types.GroupServer, types.BroadcastServer, types.NewsletterServer:
		return sender.String(), ""
	}

	// hosted.lid is a LID namespace too. Checking only HiddenUserServer treated
	// "123:8@hosted.lid" as a phone number named "123".
	if sender.Server == types.HiddenUserServer || sender.Server == types.HostedLIDServer {
		bare := sender.ToNonAD()
		bare.Server = types.HiddenUserServer
		lidUser := bare.User
		if pn, err := client.Store.LIDs.GetPNForLID(ctx, bare); err == nil && !pn.IsEmpty() {
			return pn.User, lidUser
		}
		return lidUser + "@lid", lidUser
	}

	// DefaultUserServer, and "hosted" which is the phone-side equivalent.
	bare := sender.ToNonAD()
	bare.Server = types.DefaultUserServer
	phoneUser := bare.User
	if lid, err := client.Store.LIDs.GetLIDForPN(ctx, bare); err == nil && !lid.IsEmpty() {
		return phoneUser, lid.User
	}
	return phoneUser, ""
}

// isIdentifierNotName reports whether a stored chat "name" is really just an
// identifier wearing a name's clothes — an all-digit string (a phone number or a
// LID), or the chat's own JID user. These are the values that make a failed
// lookup indistinguishable from a successful one.
func isIdentifierNotName(name string, jid types.JID) bool {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return true
	}
	if trimmed == jid.User || trimmed == jid.String() {
		return true
	}
	// Only DMs. A group can legitimately be named "1234567890" or "2026080612",
	// and a group subject is never an identifier we invented — it comes from the
	// server. The identifier-as-name bug only ever affected contact chats.
	if jid.Server == types.GroupServer || jid.Server == types.BroadcastServer ||
		jid.Server == types.NewsletterServer {
		return false
	}
	// A bare run of digits long enough to be a phone number or a LID.
	if len(trimmed) >= 10 {
		for _, r := range trimmed {
			if r < '0' || r > '9' {
				return false
			}
		}
		return true
	}
	return false
}

func GetChatName(client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	// First, check if chat already exists in database with a name.
	// A cached name that is really just an ID is worse than no name: it looks
	// like a resolved answer to every caller (get_contact returns it verbatim),
	// which is how the Buddy account's LID ended up being read as KC's identity.
	// Treat those as unset and try to resolve properly again.
	var existingName string
	err := messageStore.db.QueryRow("SELECT name FROM chats WHERE jid = ?", chatJID).Scan(&existingName)
	if err == nil && existingName != "" && !isIdentifierNotName(existingName, jid) {
		// Chat exists with a name, use that
		logger.Infof("Using existing chat name for %s: %s", chatJID, existingName)
		return existingName
	}

	// Need to determine chat name
	var name string

	if jid.Server == "g.us" {
		// This is a group chat
		logger.Infof("Getting name for group: %s", chatJID)

		// Use conversation data if provided (from history sync)
		if conversation != nil {
			// Extract name from conversation if available
			// This uses type assertions to handle different possible types
			var displayName, convName *string
			// Try to extract the fields we care about regardless of the exact type
			v := reflect.ValueOf(conversation)
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()

				// Try to find DisplayName field
				if displayNameField := v.FieldByName("DisplayName"); displayNameField.IsValid() && displayNameField.Kind() == reflect.Ptr && !displayNameField.IsNil() {
					dn := displayNameField.Elem().String()
					displayName = &dn
				}

				// Try to find Name field
				if nameField := v.FieldByName("Name"); nameField.IsValid() && nameField.Kind() == reflect.Ptr && !nameField.IsNil() {
					n := nameField.Elem().String()
					convName = &n
				}
			}

			// Use the name we found
			if displayName != nil && *displayName != "" {
				name = *displayName
			} else if convName != nil && *convName != "" {
				name = *convName
			}
		}

		// If we didn't get a name, try group info
		if name == "" {
			groupInfo, err := client.GetGroupInfo(context.Background(), jid)
			if err == nil && groupInfo.Name != "" {
				name = groupInfo.Name
			} else {
				// Fallback name for groups
				name = fmt.Sprintf("Group %s", jid.User)
			}
		}

		logger.Infof("Using group name: %s", name)
	} else {
		// This is an individual contact
		logger.Infof("Getting name for contact: %s", chatJID)

		// Prefer a real, human name. Never invent one out of an identifier:
		// `name = sender` used to run here, which is how KC's DM thread ended up
		// named "81141293912122" (the Buddy account's LID, because Buddy happened
		// to send the first stored message in that thread). Downstream that reads
		// as a resolved contact name and misidentifies the person.
		contact, err := client.Store.Contacts.GetContact(context.Background(), jid)
		if err == nil {
			switch {
			case contact.FullName != "":
				name = contact.FullName
			case contact.BusinessName != "":
				name = contact.BusinessName
			case contact.PushName != "":
				name = contact.PushName
			}
		}
		// If it is a LID, the phone number is a far better label than the LID —
		// at least it is the identifier a human recognises.
		if name == "" && jid.Server == types.HiddenUserServer {
			if pn, pnErr := client.Store.LIDs.GetPNForLID(context.Background(), jid); pnErr == nil && !pn.IsEmpty() {
				name = pn.User
			}
		}
		// Otherwise leave it EMPTY. handleMessage fills it in from PushName on the
		// next inbound message, and an empty name lets callers fall back to the JID
		// instead of trusting a number that looks like a name.

		logger.Infof("Using contact name: %s", name)
	}

	return name
}

// Handle history sync events
func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger) {
	fmt.Printf("Received history sync event with %d conversations\n", len(historySync.Data.Conversations))

	syncedCount := 0
	for _, conversation := range historySync.Data.Conversations {
		// Parse JID from the conversation
		if conversation.ID == nil {
			continue
		}

		rawChatJID := *conversation.ID

		// Try to parse the JID
		jid, err := types.ParseJID(rawChatJID)
		if err != nil {
			logger.Warnf("Failed to parse JID %s: %v", rawChatJID, err)
			continue
		}

		// Resolve LID-based chats to phone-based JIDs.
		// History sync doesn't carry SenderAlt, so rely on the
		// LID store mapping populated during live message handling.
		resolved := resolveLIDChat(client, jid, types.EmptyJID, types.EmptyJID, false)
		chatJID := resolved.String()

		// Get appropriate chat name by passing the history sync conversation directly
		name := GetChatName(client, messageStore, resolved, chatJID, conversation, "", logger)

		// Process messages
		messages := conversation.Messages
		if len(messages) > 0 {
			// Update chat with latest message timestamp
			latestMsg := messages[0]
			if latestMsg == nil || latestMsg.Message == nil {
				continue
			}

			// Get timestamp from message info
			ts := latestMsg.Message.GetMessageTimestamp()
			if ts == 0 {
				continue
			}
			timestamp := time.Unix(int64(ts), 0)

			_ = messageStore.StoreChat(chatJID, name, timestamp)

			// Store messages
			for _, msg := range messages {
				if msg == nil || msg.Message == nil {
					continue
				}

				// Extract text content
				var content string
				if msg.Message.Message != nil {
					if conv := msg.Message.Message.GetConversation(); conv != "" {
						content = conv
					} else if ext := msg.Message.Message.GetExtendedTextMessage(); ext != nil {
						content = ext.GetText()
					}
				}

				// Extract media info - use message timestamp for consistent filenames
				var mediaType, filename, url string
				var mediaKey, fileSHA256, fileEncSHA256 []byte
				var fileLength uint64

				if msg.Message.Message != nil {
					mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(msg.Message.Message, timestamp)
				}

				// Log the message content for debugging
				logger.Infof("Message content: %v, Media Type: %v", content, mediaType)

				// Skip messages with no content and no media
				if content == "" && mediaType == "" {
					continue
				}

				// Determine sender
				var sender string
				isFromMe := false
				if msg.Message.Key != nil {
					if msg.Message.Key.FromMe != nil {
						isFromMe = *msg.Message.Key.FromMe
					}
					if !isFromMe && msg.Message.Key.Participant != nil && *msg.Message.Key.Participant != "" {
						sender = *msg.Message.Key.Participant
					} else if isFromMe {
						sender = client.Store.ID.User
					} else {
						sender = jid.User
					}
				} else {
					sender = jid.User
				}

				// History sync hands back raw JID strings (a bare user, or a full
				// "…@lid" / "…@s.whatsapp.net"). Put them through the same
				// normalisation as live messages so a person has one identity
				// regardless of which path stored them.
				senderLID := ""
				if parsed, parseErr := types.ParseJID(sender); parseErr == nil && !parsed.IsEmpty() {
					sender, senderLID = normalizeSender(client, parsed)
				} else if parsed, parseErr := types.ParseJID(sender + "@s.whatsapp.net"); parseErr == nil {
					sender, senderLID = normalizeSender(client, parsed)
				}

				// Store message
				msgID := ""
				if msg.Message.Key != nil && msg.Message.Key.ID != nil {
					msgID = *msg.Message.Key.ID
				}

				// Get message timestamp
				ts := msg.Message.GetMessageTimestamp()
				if ts == 0 {
					continue
				}
				msgTimestamp := time.Unix(int64(ts), 0)

				err = messageStore.StoreMessage(
					msgID,
					chatJID,
					sender,
					senderLID,
					content,
					msgTimestamp,
					isFromMe,
					mediaType,
					filename,
					url,
					mediaKey,
					fileSHA256,
					fileEncSHA256,
					fileLength,
				)
				if err != nil {
					logger.Warnf("Failed to store history message: %v", err)
				} else {
					syncedCount++
					// Log successful message storage
					if mediaType != "" {
						logger.Infof("Stored message: [%s] %s -> %s: [%s: %s] %s",
							msgTimestamp.Format("2006-01-02 15:04:05"), sender, chatJID, mediaType, filename, content)
					} else {
						logger.Infof("Stored message: [%s] %s -> %s: %s",
							msgTimestamp.Format("2006-01-02 15:04:05"), sender, chatJID, content)
					}
				}
			}
		}
	}

	fmt.Printf("History sync complete. Stored %d messages.\n", syncedCount)
}

// analyzeOggOpus tries to extract duration and generate a simple waveform from an Ogg Opus file
func analyzeOggOpus(data []byte) (duration uint32, waveform []byte, err error) {
	// Try to detect if this is a valid Ogg file by checking for the "OggS" signature
	// at the beginning of the file
	if len(data) < 4 || string(data[0:4]) != "OggS" {
		return 0, nil, fmt.Errorf("not a valid Ogg file (missing OggS signature)")
	}

	// Parse Ogg pages to find the last page with a valid granule position
	var lastGranule uint64
	var sampleRate uint32 = 48000 // Default Opus sample rate
	var preSkip uint16 = 0
	var foundOpusHead bool

	// Scan through the file looking for Ogg pages
	for i := 0; i < len(data); {
		// Check if we have enough data to read Ogg page header
		if i+27 >= len(data) {
			break
		}

		// Verify Ogg page signature
		if string(data[i:i+4]) != "OggS" {
			// Skip until next potential page
			i++
			continue
		}

		// Extract header fields
		granulePos := binary.LittleEndian.Uint64(data[i+6 : i+14])
		pageSeqNum := binary.LittleEndian.Uint32(data[i+18 : i+22])
		numSegments := int(data[i+26])

		// Extract segment table
		if i+27+numSegments >= len(data) {
			break
		}
		segmentTable := data[i+27 : i+27+numSegments]

		// Calculate page size
		pageSize := 27 + numSegments
		for _, segLen := range segmentTable {
			pageSize += int(segLen)
		}

		// Check if we're looking at an OpusHead packet (should be in first few pages)
		if !foundOpusHead && pageSeqNum <= 1 {
			// Look for "OpusHead" marker in this page
			pageData := data[i : i+pageSize]
			headPos := bytes.Index(pageData, []byte("OpusHead"))
			if headPos >= 0 && headPos+12 < len(pageData) {
				// Found OpusHead, extract sample rate and pre-skip
				// OpusHead format: Magic(8) + Version(1) + Channels(1) + PreSkip(2) + SampleRate(4) + ...
				headPos += 8 // Skip "OpusHead" marker
				// PreSkip is 2 bytes at offset 10
				if headPos+12 <= len(pageData) {
					preSkip = binary.LittleEndian.Uint16(pageData[headPos+10 : headPos+12])
					sampleRate = binary.LittleEndian.Uint32(pageData[headPos+12 : headPos+16])
					foundOpusHead = true
					fmt.Printf("Found OpusHead: sampleRate=%d, preSkip=%d\n", sampleRate, preSkip)
				}
			}
		}

		// Keep track of last valid granule position
		if granulePos != 0 {
			lastGranule = granulePos
		}

		// Move to next page
		i += pageSize
	}

	if !foundOpusHead {
		fmt.Println("Warning: OpusHead not found, using default values")
	}

	// Calculate duration based on granule position
	if lastGranule > 0 {
		// Formula for duration: (lastGranule - preSkip) / sampleRate
		durationSeconds := float64(lastGranule-uint64(preSkip)) / float64(sampleRate)
		duration = uint32(math.Ceil(durationSeconds))
		fmt.Printf("Calculated Opus duration from granule: %f seconds (lastGranule=%d)\n",
			durationSeconds, lastGranule)
	} else {
		// Fallback to rough estimation if granule position not found
		fmt.Println("Warning: No valid granule position found, using estimation")
		durationEstimate := float64(len(data)) / 2000.0 // Very rough approximation
		duration = uint32(durationEstimate)
	}

	// Make sure we have a reasonable duration (at least 1 second, at most 300 seconds)
	if duration < 1 {
		duration = 1
	} else if duration > 300 {
		duration = 300
	}

	// Generate waveform
	waveform = placeholderWaveform(duration)

	fmt.Printf("Ogg Opus analysis: size=%d bytes, calculated duration=%d sec, waveform=%d bytes\n",
		len(data), duration, len(waveform))

	return duration, waveform, nil
}

// min returns the smaller of x or y
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// placeholderWaveform generates a synthetic waveform for WhatsApp voice messages
// that appears natural with some variability based on the duration
func placeholderWaveform(duration uint32) []byte {
	// WhatsApp expects a 64-byte waveform for voice messages
	const waveformLength = 64
	waveform := make([]byte, waveformLength)

	// Seed the random number generator for consistent results with the same duration
	rand.Seed(int64(duration))

	// Create a more natural looking waveform with some patterns and variability
	// rather than completely random values

	// Base amplitude and frequency - longer messages get faster frequency
	baseAmplitude := 35.0
	frequencyFactor := float64(min(int(duration), 120)) / 30.0

	for i := range waveform {
		// Position in the waveform (normalized 0-1)
		pos := float64(i) / float64(waveformLength)

		// Create a wave pattern with some randomness
		// Use multiple sine waves of different frequencies for more natural look
		val := baseAmplitude * math.Sin(pos*math.Pi*frequencyFactor*8)
		val += (baseAmplitude / 2) * math.Sin(pos*math.Pi*frequencyFactor*16)

		// Add some randomness to make it look more natural
		val += (rand.Float64() - 0.5) * 15

		// Add some fade-in and fade-out effects
		fadeInOut := math.Sin(pos * math.Pi)
		val = val * (0.7 + 0.3*fadeInOut)

		// Center around 50 (typical voice baseline)
		val = val + 50

		// Ensure values stay within WhatsApp's expected range (0-100)
		if val < 0 {
			val = 0
		} else if val > 100 {
			val = 100
		}

		waveform[i] = byte(val)
	}

	return waveform
}
