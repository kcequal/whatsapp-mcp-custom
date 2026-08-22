# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0](https://github.com/kcequal/whatsapp-mcp-custom/compare/v0.1.0...v0.1.0) (2026-08-22)


### release

* adopt Release Please for automated versioning/changelog ([#15](https://github.com/kcequal/whatsapp-mcp-custom/issues/15)) ([6d45958](https://github.com/kcequal/whatsapp-mcp-custom/commit/6d45958139effa3079ff27a9708d400f89ba9ddf))


### Features

* bridge returns the provider message_id on /api/send ([c10d612](https://github.com/kcequal/whatsapp-mcp-custom/commit/c10d612dfe24be15343e4577d63339feb140792e))
* default the pairing number to KC_BOT_PHONE ([309f55b](https://github.com/kcequal/whatsapp-mcp-custom/commit/309f55b98efb049b087f6340bfbc4a7ebc847749))
* Equal custom WhatsApp tools — messaging, groups, history, transcription, search ([d7131b2](https://github.com/kcequal/whatsapp-mcp-custom/commit/d7131b266f418283d047e183533ae2e16554240c))
* fire webhook for media messages + harden media downloads ([708c96c](https://github.com/kcequal/whatsapp-mcp-custom/commit/708c96cd885c70225b94a7158c01198e68ed33c3))
* link by pairing code via PAIR_PHONE instead of a QR scan ([fc99db3](https://github.com/kcequal/whatsapp-mcp-custom/commit/fc99db38a1f428b485aa9d30c4b0cf6518d8a696))
* report forwarding provenance on the inbound webhook ([44153ad](https://github.com/kcequal/whatsapp-mcp-custom/commit/44153ad89c47bddb0ddf041355c34252283ab248))


### Bug Fixes

* address round-2 QA findings, including two of my own bad fixes ([b56913b](https://github.com/kcequal/whatsapp-mcp-custom/commit/b56913ba3ad8cfc61d334993c7ed363364920c9a))
* address the 2026-08-06 Codex QA findings ([e6b95d4](https://github.com/kcequal/whatsapp-mcp-custom/commit/e6b95d4797a4ef4a6962834f510ede0ee885331b))
* captions, unconditional media download, and honest chat names ([384700f](https://github.com/kcequal/whatsapp-mcp-custom/commit/384700f2827fa2a10612fc18edb8799aba0c4ee7))
* explicit [@lid](https://github.com/lid) suffix outranks inferred map membership ([dad4aab](https://github.com/kcequal/whatsapp-mcp-custom/commit/dad4aab010ad405f9605518591de2460ad4a4412))
* keep signed oh/oe tokens on media directPath to stop CDN 403 ([9845980](https://github.com/kcequal/whatsapp-mcp-custom/commit/9845980c78e56890e1653f0fc12dbb0ab3d89a7e))
* normalise sender to the phone JID, keep the LID as an alias ([6e68118](https://github.com/kcequal/whatsapp-mcp-custom/commit/6e681185d718d2017ed39f3211d01a57164d533c))
* record the bridge's own outgoing messages in messages.db ([9a85451](https://github.com/kcequal/whatsapp-mcp-custom/commit/9a85451e08752454eb37ed06b01ecb32a1f5533b))
* round-3 QA findings, incl. search index left stale by the LID chat migration ([9298979](https://github.com/kcequal/whatsapp-mcp-custom/commit/9298979413b10c0307a66f494bf8d9c56a9b85f4))
* round-4 QA findings — namespace ambiguity, alias homogenisation, index baseline ([ac6eb1b](https://github.com/kcequal/whatsapp-mcp-custom/commit/ac6eb1bfaac314c1a227223c0d26bbfc44eb0d45))
* serialize media downloads per message + unique temp files ([721e8a6](https://github.com/kcequal/whatsapp-mcp-custom/commit/721e8a631c9b14fc068e2c3039d3b7774e342467))


### Documentation

* message_id is client-generated, not minted by WhatsApp ([8264eac](https://github.com/kcequal/whatsapp-mcp-custom/commit/8264eac0fc7766e23074903bc4313c50f0fa57ef))

## 0.1.0 (2026-03-02)

### Added

**Go Bridge:**

- `/api/typing` endpoint - Send typing indicators to chats
- `/api/health` endpoint - Check WhatsApp connection status
- Webhook system for incoming messages (`webhook.go`)
  - Configurable via `WEBHOOK_URL` environment variable
  - Includes quoted message info (reply context)
  - `FORWARD_SELF` option to include self-sent messages
- Auto-download of media files when messages arrive
- Quoted message extraction (reply-to context: ID, sender, content)
- Connection status checks before API operations
- HTTP server timeouts for stability (read: 30s, write: 60s, idle: 120s)

**Python MCP Server:**

- `get_contact` tool - Resolve phone number to contact name
- `sender_display` field in messages - Shows "Name (phone)" format
- `sender_phone` field - Extracted phone number from JID
- Environment variable configuration (`WHATSAPP_DB_PATH`, `WHATSAPP_API_URL`)
- Test suite with pytest (8 tests)
- `sort_by` parameter for `list_messages` ("newest" or "oldest")
- Increased default limits (20 → 50 messages/chats)
- Maximum limits (500 messages, 200 chats)

**Infrastructure:**

- CI/CD pipeline with GitHub Actions (lint, build, test)
- Release Please workflow for automated release PRs/tags/changelog (`.github/workflows/release-please.yml`)
- Manual fallback release workflow for artifact re-publish (`.github/workflows/release.yml`)
- Version consistency check across `pyproject.toml`, `server.json`, and release tags
- Comprehensive documentation (README, CLAUDE.md)
- Maintainer release playbook (`docs/RELEASING.md`)
- Environment variable examples (`.env.example`)

### Fixed

- Go compilation errors from whatsmeow API changes (added `context.Background()`)
- Media filename consistency - now uses message timestamp instead of download time
- Startup migration now consolidates legacy `@lid` chat/message rows into mapped phone JIDs (`@s.whatsapp.net`) to prevent split chat history
- golangci.yml configuration (removed deprecated linters)
- Python linting issues (185 errors fixed)
- Trailing whitespace in SQL queries

### Changed

- Upgraded whatsmeow to latest version (v0.0.0-20260107124630)
- Upgraded Go linting pipeline to golangci-lint v2 (CI action + v2 config schema)
- Modernized Python type hints (`Optional[str]` → `str | None`)
- Improved docstrings with date format examples
- Media download uses message timestamp for consistent filenames
- Refactored `send_message` to use unified `recipient` parameter
- Adopted Release Please for automated versioning/changelog ([`#15`](https://github.com/verygoodplugins/whatsapp-mcp/issues/15)) ([6d45958](https://github.com/verygoodplugins/whatsapp-mcp/commit/6d45958139effa3079ff27a9708d400f89ba9ddf))

### Removed

- Unused `package.json` from Go bridge (was for wrangler, not needed)
- Compiled binary from git tracking

---

## Fork History

This project is a fork of [lharries/whatsapp-mcp](https://github.com/lharries/whatsapp-mcp), originally created by [Luke Harries](https://github.com/lharries) in 2024.

The original repository was last updated in April 2025. This fork by [Very Good Plugins](https://verygoodplugins.com) continues active development with bug fixes, new features, and MCP registry publication.
