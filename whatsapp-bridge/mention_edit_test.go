package main

// /api/edit used to strip @-tags. Found live on 2026-08-27: editing a tagged
// group message rewrote Keshav's mention as the literal text
// "@147609603813461" in "KC <> KR". The send path was always correct
// (extractMentions -> ContextInfo.MentionedJid); the edit path built a plain
// Conversation message, and Conversation has no field a mention can live in,
// so every edit of a tagged message silently destroyed the tag.
//
// Two tests, because either one alone can be satisfied while the bug is live:
// the first pins the SHAPE decision, the second pins that /api/edit actually
// USES it. A helper that gets the shape right and a handler that ignores it is
// exactly the state this fixes.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// The shape decision. Text carrying @<lid> must go as an ExtendedTextMessage
// with the JID in MentionedJid, because that is the only shape receiving
// clients render a clickable tag from.
func TestNewTextMessage_TaggedTextCarriesMentions(t *testing.T) {
	msg := newTextMessage("morning @147609603813461 can you look at this")

	ext := msg.GetExtendedTextMessage()
	if ext == nil {
		t.Fatalf("tagged text was built as a plain Conversation (%q) — Conversation has no field "+
			"for mentions, so the tag renders as literal text", msg.GetConversation())
	}
	if got := ext.GetText(); got != "morning @147609603813461 can you look at this" {
		t.Errorf("text = %q, want it unaltered", got)
	}
	got := ext.GetContextInfo().GetMentionedJID()
	if len(got) != 1 || got[0] != "147609603813461@lid" {
		t.Errorf("MentionedJid = %v, want [147609603813461@lid] — without it the client shows raw text", got)
	}
}

// Several tags in one message all survive, and a repeat is not sent twice.
func TestNewTextMessage_MultipleMentions(t *testing.T) {
	msg := newTextMessage("@147609603813461 and @185366493536339 — also @147609603813461 again")

	got := msg.GetExtendedTextMessage().GetContextInfo().GetMentionedJID()
	want := []string{"147609603813461@lid", "185366493536339@lid"}
	if len(got) != len(want) {
		t.Fatalf("MentionedJid = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("MentionedJid = %v, want %v", got, want)
		}
	}
}

// Untagged text must stay a plain Conversation. Promoting every message to
// ExtendedTextMessage would change the wire shape of ordinary edits for no
// reason, and an empty MentionedJid is not the same as no mention block.
func TestNewTextMessage_PlainTextStaysConversation(t *testing.T) {
	msg := newTextMessage("just a normal correction")

	if ext := msg.GetExtendedTextMessage(); ext != nil {
		t.Fatalf("plain text was promoted to ExtendedTextMessage (%q); it should stay a Conversation",
			ext.GetText())
	}
	if got := msg.GetConversation(); got != "just a normal correction" {
		t.Errorf("Conversation = %q, want the text unaltered", got)
	}
}

// THE REGRESSION GUARD. The bug was never in a helper — it was /api/edit
// hardcoding &waProto.Message{Conversation: ...} at the BuildEdit call. The
// three tests above all pass against the broken tree, because newTextMessage
// simply was not wired in.
//
// This is a SOURCE-level (AST) test because the handler needs a logged-in
// whatsmeow client to drive at runtime — client.BuildEdit is a method on a
// concrete *whatsmeow.Client with no seam to inject, and the same reasoning is
// already recorded in message_id_test.go for sendWhatsAppMessage. It is not a
// substitute for watching a live edit render a tag; that remains unproven until
// someone edits a tagged message on a connected bridge. It pins the wiring by
// identifier, not by source text, so reformatting cannot disarm it.
func TestEditHandler_BuildsContentViaNewTextMessage(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing main.go: %v", err)
	}

	// The FuncLit containing the client.BuildEdit call is the /api/edit handler.
	var handler *ast.FuncLit
	var buildEdit *ast.CallExpr
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.FuncLit)
		if !ok {
			return true
		}
		ast.Inspect(lit.Body, func(m ast.Node) bool {
			call, ok := m.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "BuildEdit" {
				handler, buildEdit = lit, call
				return false
			}
			return true
		})
		return handler == nil
	})
	if handler == nil {
		t.Fatal("no client.BuildEdit call found in main.go — the /api/edit handler has moved or gone, " +
			"and this regression guard has stopped guarding anything")
	}
	if len(buildEdit.Args) != 3 {
		t.Fatalf("BuildEdit called with %d args, want 3 (chat, messageID, newContent)", len(buildEdit.Args))
	}

	// Resolve the third argument to what produced it.
	content := buildEdit.Args[2]
	if ident, ok := content.(*ast.Ident); ok {
		var rhs ast.Expr
		ast.Inspect(handler.Body, func(m ast.Node) bool {
			assign, ok := m.(*ast.AssignStmt)
			if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
				return true
			}
			if lhs, ok := assign.Lhs[0].(*ast.Ident); ok && lhs.Name == ident.Name {
				rhs = assign.Rhs[0]
				return false
			}
			return true
		})
		if rhs == nil {
			t.Fatalf("could not find where %q is assigned in the /api/edit handler", ident.Name)
		}
		content = rhs
	}

	call, ok := content.(*ast.CallExpr)
	if !ok {
		t.Fatalf("%s: /api/edit builds its content as %s, want newTextMessage(...). A literal "+
			"waProto.Message here cannot carry mentions, which is the bug: editing a tagged "+
			"message rewrites the tag as literal text",
			fset.Position(buildEdit.Pos()), exprText(fset, content))
	}
	fn, ok := call.Fun.(*ast.Ident)
	if !ok || fn.Name != "newTextMessage" {
		t.Fatalf("%s: /api/edit builds its content with %s, want newTextMessage(...) — the only "+
			"path that chooses the mention-carrying wire shape",
			fset.Position(buildEdit.Pos()), exprText(fset, call.Fun))
	}
}
