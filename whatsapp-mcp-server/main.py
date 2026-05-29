import signal
import sys
from typing import Any

from mcp.server.fastmcp import FastMCP

from whatsapp import (
    download_media as whatsapp_download_media,
)
from whatsapp import (
    get_chat as whatsapp_get_chat,
)
from whatsapp import (
    get_contact_chats as whatsapp_get_contact_chats,
)
from whatsapp import (
    get_direct_chat_by_contact as whatsapp_get_direct_chat_by_contact,
)
from whatsapp import (
    get_last_interaction as whatsapp_get_last_interaction,
)
from whatsapp import (
    get_message_context as whatsapp_get_message_context,
)
from whatsapp import (
    get_sender_name as whatsapp_get_sender_name,
)
from whatsapp import (
    list_chats as whatsapp_list_chats,
)
from whatsapp import (
    list_messages as whatsapp_list_messages,
)
from whatsapp import (
    search_contacts as whatsapp_search_contacts,
)
from whatsapp import (
    send_audio_message as whatsapp_audio_voice_message,
)
from whatsapp import (
    send_file as whatsapp_send_file,
)
from whatsapp import (
    edit_message as whatsapp_edit_message,
)
from whatsapp import (
    forward_message as whatsapp_forward_message,
)
from whatsapp import (
    get_group_info as whatsapp_get_group_info,
)
from whatsapp import (
    list_recent_calls as whatsapp_list_recent_calls,
)
from whatsapp import (
    mark_chat_read as whatsapp_mark_chat_read,
)
from whatsapp import (
    mark_messages_read as whatsapp_mark_messages_read,
)
from whatsapp import (
    react_to_message as whatsapp_react_to_message,
)
from whatsapp import (
    reply_to_message as whatsapp_reply_to_message,
)
from whatsapp import (
    revoke_message as whatsapp_revoke_message,
)
from whatsapp import (
    send_location as whatsapp_send_location,
)
from whatsapp import (
    send_sticker as whatsapp_send_sticker,
)
from whatsapp import (
    update_group_participants as whatsapp_update_group_participants,
)
from whatsapp import (
    send_message as whatsapp_send_message,
)

# Initialize FastMCP server
mcp = FastMCP("whatsapp")


@mcp.tool()
def search_contacts(query: str) -> list[dict[str, Any]]:
    """Search WhatsApp contacts by name or phone number.

    Args:
        query: Search term to match against contact names or phone numbers
    """
    contacts = whatsapp_search_contacts(query)
    return contacts


@mcp.tool()
def get_contact(
    identifier: str | None = None,
    phone_number: str | None = None,
    phone: str | None = None,
) -> dict[str, Any]:
    """Look up a WhatsApp contact by phone number, LID, or full JID.

    Automatically detects the identifier type and queries appropriately.

    Args:
        identifier: Phone number, LID, or full JID. Examples:
                    - "12025551234" (phone number)
                    - "184125298348272" (LID - long numeric)
                    - "12025551234@s.whatsapp.net" (phone JID)
                    - "184125298348272@lid" (LID JID)
        phone_number: Backward-compatible alias for `identifier`.
        phone: Backward-compatible alias for `identifier` (matches README parameter name).

    Returns:
        Dictionary with jid, name, display_name, is_lid, and resolved status
    """
    if identifier is None:
        identifier = phone_number
    if identifier is None:
        identifier = phone
    if identifier is None:
        raise ValueError("Missing required argument: identifier (or phone_number / phone)")

    identifier = identifier.strip()
    if not identifier:
        raise ValueError("identifier must be non-empty")

    # Detect identifier type and normalize to JID.
    if "@" in identifier:
        # Already a JID - use as-is
        jid = identifier
        is_lid = jid.endswith("@lid") or jid.split("@", 1)[-1] == "lid"
    else:
        digits = "".join(c for c in identifier if c.isdigit())
        if digits:
            # WhatsApp phone numbers are max 15 digits (E.164). Longer numeric IDs are typically LIDs.
            # For 15-digit numbers, ambiguity exists (could be phone or LID), so we try phone first and
            # fall back to LID if nothing is found.
            if len(digits) > 15:
                jid = f"{digits}@lid"
                is_lid = True
            else:
                jid = f"{digits}@s.whatsapp.net"
                is_lid = False
        else:
            # Non-numeric and not a JID; try as-is.
            jid = identifier
            is_lid = False

    jid_user = jid.split("@", 1)[0]

    display_name: str | None = None
    resolved = False

    # Prefer chats table lookup via get_chat (works for both phone and LID contacts).
    candidates: list[tuple[str, bool]] = [(jid, is_lid)]
    if "@" not in identifier and identifier.isdigit() and len(identifier) == 15:
        # 15-digit numeric identifier is ambiguous (could be phone or LID).
        # Try LID JID as a fallback if phone JID isn't found.
        candidates.append((f"{identifier}@lid", True))

    chat = None
    for candidate_jid, candidate_is_lid in candidates:
        chat = whatsapp_get_chat(candidate_jid, include_last_message=False)
        if chat:
            jid = candidate_jid
            is_lid = candidate_is_lid
            jid_user = jid.split("@", 1)[0]
            break

    if chat and chat.get("name"):
        display_name = chat["name"]
        resolved = display_name not in (jid, jid_user)
    else:
        # Fallback: best-effort sender-name resolution (may use fuzzy LIKE lookup).
        display_name = whatsapp_get_sender_name(jid)
        resolved = display_name not in (jid, jid_user, identifier)

    return {
        "identifier": identifier,
        "jid": jid,
        "phone_number": jid_user if not is_lid else None,
        "lid": jid_user if is_lid else None,
        "name": display_name if resolved else jid_user,
        "display_name": display_name,
        "is_lid": is_lid,
        "resolved": resolved,
    }


@mcp.tool()
def list_messages(
    after: str | None = None,
    before: str | None = None,
    sender_phone_number: str | None = None,
    chat_jid: str | None = None,
    query: str | None = None,
    limit: int = 50,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1,
    sort_by: str = "newest",
) -> list[dict[str, Any]]:
    """Get WhatsApp messages matching specified criteria with optional context.

    Each message includes sender_display showing "Name (phone)" for easy identification.

    Args:
        after: ISO-8601 date string (e.g., "2026-01-01" or "2026-01-01T09:00:00")
        before: ISO-8601 date string (e.g., "2026-01-09" or "2026-01-09T18:00:00")
        sender_phone_number: Phone number to filter by sender (e.g., "12025551234")
        chat_jid: Chat JID to filter by (e.g., "12025551234@s.whatsapp.net" or group JID)
        query: Search term to filter messages by content
        limit: Max messages to return (default 50, max 500)
        page: Page number for pagination (default 0)
        include_context: Include surrounding messages for context (default True)
        context_before: Messages to include before each match (default 1)
        context_after: Messages to include after each match (default 1)
        sort_by: "newest" (default, most recent first) or "oldest" (chronological)
    """
    # Cap limit at 500 to prevent excessive queries
    limit = min(limit, 500)
    messages = whatsapp_list_messages(
        after=after,
        before=before,
        sender_phone_number=sender_phone_number,
        chat_jid=chat_jid,
        query=query,
        limit=limit,
        page=page,
        include_context=include_context,
        context_before=context_before,
        context_after=context_after,
        sort_by=sort_by,
    )
    return messages


@mcp.tool()
def list_chats(
    query: str | None = None,
    limit: int = 50,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active",
) -> list[dict[str, Any]]:
    """Get WhatsApp chats matching specified criteria.

    Args:
        query: Search term to filter chats by name or JID
        limit: Max chats to return (default 50, max 200)
        page: Page number for pagination (default 0)
        include_last_message: Include the last message in each chat (default True)
        sort_by: "last_active" (default, most recent first) or "name" (alphabetical)
    """
    # Cap limit at 200 to prevent excessive queries
    limit = min(limit, 200)
    chats = whatsapp_list_chats(
        query=query, limit=limit, page=page, include_last_message=include_last_message, sort_by=sort_by
    )
    return chats


@mcp.tool()
def get_chat(chat_jid: str, include_last_message: bool = True) -> dict[str, Any]:
    """Get WhatsApp chat metadata by JID.

    Args:
        chat_jid: The JID of the chat to retrieve
        include_last_message: Whether to include the last message (default True)
    """
    chat = whatsapp_get_chat(chat_jid, include_last_message)
    return chat


@mcp.tool()
def get_direct_chat_by_contact(sender_phone_number: str) -> dict[str, Any]:
    """Get WhatsApp chat metadata by sender phone number.

    Args:
        sender_phone_number: The phone number to search for
    """
    chat = whatsapp_get_direct_chat_by_contact(sender_phone_number)
    return chat


@mcp.tool()
def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> list[dict[str, Any]]:
    """Get all WhatsApp chats involving the contact.

    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    chats = whatsapp_get_contact_chats(jid, limit, page)
    return chats


@mcp.tool()
def get_last_interaction(jid: str) -> dict[str, Any]:
    """Get most recent WhatsApp message involving the contact.

    Args:
        jid: The JID of the contact to search for

    Returns:
        Message dictionary with id, timestamp, sender, content, etc. or empty dict if not found.
    """
    message = whatsapp_get_last_interaction(jid)
    return message if message else {}


@mcp.tool()
def get_message_context(message_id: str, before: int = 5, after: int = 5) -> dict[str, Any]:
    """Get context around a specific WhatsApp message.

    Args:
        message_id: The ID of the message to get context for
        before: Number of messages to include before the target message (default 5)
        after: Number of messages to include after the target message (default 5)
    """
    context = whatsapp_get_message_context(message_id, before, after)
    return context


@mcp.tool()
def send_message(recipient: str, message: str) -> dict[str, Any]:
    """Send a WhatsApp message to a person or group. For group chats use the JID.

    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        message: The message text to send

    Returns:
        A dictionary containing success status and a status message
    """
    # Validate input
    if not recipient:
        return {"success": False, "message": "Recipient must be provided"}

    # Call the whatsapp_send_message function with the unified recipient parameter
    success, status_message = whatsapp_send_message(recipient, message)
    return {"success": success, "message": status_message}


@mcp.tool()
def reply_to_message(
    recipient: str, message: str, reply_to_message_id: str, reply_to_sender: str = ""
) -> dict[str, Any]:
    """Send a text message that quotes a parent message (proper threaded reply).

    Args:
        recipient: Phone (no +) or JID of the chat to reply in.
        message: The reply text.
        reply_to_message_id: The ID of the message being replied to.
        reply_to_sender: JID of the original sender. Required for groups; for 1:1 chats
                        it can be left blank and will default to the chat JID.
    """
    success, status = whatsapp_reply_to_message(
        recipient, message, reply_to_message_id, reply_to_sender
    )
    return {"success": success, "message": status}


@mcp.tool()
def forward_message(
    recipient: str, message_id: str, source_chat_jid: str
) -> dict[str, Any]:
    """Forward an existing TEXT message to another chat with the "Forwarded" badge.

    For media messages, use download_media + send_file from the calling side instead.

    Args:
        recipient: Where to forward the message to.
        message_id: ID of the message to forward.
        source_chat_jid: JID of the chat the original message lives in.
    """
    success, status = whatsapp_forward_message(recipient, message_id, source_chat_jid)
    return {"success": success, "message": status}


@mcp.tool()
def edit_message(recipient: str, message_id: str, new_message: str) -> dict[str, Any]:
    """Edit a previously-sent text message. WhatsApp allows ~15 minutes after the original send.

    Args:
        recipient: The chat the message lives in.
        message_id: The ID of the message to edit.
        new_message: The new text content.
    """
    success, status = whatsapp_edit_message(recipient, message_id, new_message)
    return {"success": success, "message": status}


@mcp.tool()
def revoke_message(
    recipient: str, message_id: str, sender: str = ""
) -> dict[str, Any]:
    """Delete a sent message for everyone ("revoke"). WhatsApp allows ~2 days after send.

    Args:
        recipient: The chat the message is in.
        message_id: The ID of the message to revoke.
        sender: JID of the message sender. Defaults to self.
    """
    success, status = whatsapp_revoke_message(recipient, message_id, sender)
    return {"success": success, "message": status}


@mcp.tool()
def mark_messages_read(
    chat_jid: str,
    message_ids: list[str],
    sender_jid: str = "",
    receipt_type: str = "read",
) -> dict[str, Any]:
    """Send read (or delivered/played) receipts for one or more messages in a chat.

    Args:
        chat_jid: The JID of the chat.
        message_ids: List of message IDs to acknowledge.
        sender_jid: Original sender's JID (required for groups; defaults to the chat for DMs).
        receipt_type: "read" (default), "delivered", or "played" (for voice notes).
    """
    success, status = whatsapp_mark_messages_read(
        chat_jid, message_ids, sender_jid, receipt_type
    )
    return {"success": success, "message": status}


@mcp.tool()
def mark_chat_read(chat_jid: str) -> dict[str, Any]:
    """Batch-mark every locally-stored unread message in a chat as read.

    Args:
        chat_jid: The JID of the chat to clear unread state for.
    """
    success, status = whatsapp_mark_chat_read(chat_jid)
    return {"success": success, "message": status}


@mcp.tool()
def get_group_info(group_jid: str) -> dict[str, Any]:
    """Get full metadata for a WhatsApp group: name, topic, owner, participants and admin status.

    Args:
        group_jid: The group JID (ends with @g.us).
    """
    return whatsapp_get_group_info(group_jid)


@mcp.tool()
def update_group_participants(
    group_jid: str, action: str, jids: list[str]
) -> dict[str, Any]:
    """Add, remove, promote, or demote participants in a WhatsApp group.

    Args:
        group_jid: The group JID (ends with @g.us).
        action: One of "add", "remove", "promote", "demote".
        jids: Phone numbers (no +) or JIDs of the participants to act on.
    """
    success, status, participants = whatsapp_update_group_participants(
        group_jid, action, jids
    )
    return {"success": success, "message": status, "participants": participants}


@mcp.tool()
def send_location(
    recipient: str,
    latitude: float,
    longitude: float,
    name: str = "",
    address: str = "",
) -> dict[str, Any]:
    """Send a static location share to a chat.

    Args:
        recipient: Phone (no +) or JID of the destination chat.
        latitude: Decimal latitude (e.g. 12.9716).
        longitude: Decimal longitude (e.g. 77.5946).
        name: Optional place name.
        address: Optional postal-style address.
    """
    success, status = whatsapp_send_location(recipient, latitude, longitude, name, address)
    return {"success": success, "message": status}


@mcp.tool()
def send_sticker(recipient: str, sticker_path: str) -> dict[str, Any]:
    """Send a sticker (.webp) to a chat.

    Args:
        recipient: Phone (no +) or JID of the destination chat.
        sticker_path: Absolute path to a .webp file.
    """
    success, status = whatsapp_send_sticker(recipient, sticker_path)
    return {"success": success, "message": status}


@mcp.tool()
def list_recent_calls(limit: int = 50, after: str = "") -> dict[str, Any]:
    """List recent incoming-call events captured from the WhatsApp event stream.

    The bridge logs CallOffer / CallAccept / CallTerminate / CallReject events into a local table.
    This tool reads that table.

    Args:
        limit: Max number of events to return (default 50, max 500).
        after: Optional ISO-8601 timestamp; only events newer than this are returned.
    """
    return whatsapp_list_recent_calls(limit, after)


@mcp.tool()
def react_to_message(
    recipient: str, message_id: str, emoji: str, from_me: bool = True
) -> dict[str, Any]:
    """Send an emoji reaction to an existing WhatsApp message.

    Args:
        recipient: Phone number (no +) or full JID of the chat the message is in.
        message_id: The ID of the message to react to.
        emoji: The reaction emoji to send (e.g. "👍", "❤️", "😂"). Pass an empty string to remove a prior reaction.
        from_me: True if the original message was sent by this account, False if it was received from someone else. Default True.

    Returns:
        A dictionary containing success status and a status message.
    """
    success, status_message = whatsapp_react_to_message(recipient, message_id, emoji, from_me)
    return {"success": success, "message": status_message}


@mcp.tool()
def send_file(recipient: str, media_path: str) -> dict[str, Any]:
    """Send a file such as a picture, raw audio, video or document via WhatsApp to the specified recipient. For group messages use the JID.

    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        media_path: The absolute path to the media file to send (image, video, document)

    Returns:
        A dictionary containing success status and a status message
    """

    # Call the whatsapp_send_file function
    success, status_message = whatsapp_send_file(recipient, media_path)
    return {"success": success, "message": status_message}


@mcp.tool()
def send_audio_message(recipient: str, media_path: str) -> dict[str, Any]:
    """Send any audio file as a WhatsApp audio message to the specified recipient. For group messages use the JID. If it errors due to ffmpeg not being installed, use send_file instead.

    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        media_path: The absolute path to the audio file to send (will be converted to Opus .ogg if it's not a .ogg file)

    Returns:
        A dictionary containing success status and a status message
    """
    success, status_message = whatsapp_audio_voice_message(recipient, media_path)
    return {"success": success, "message": status_message}


@mcp.tool()
def download_media(message_id: str, chat_jid: str) -> dict[str, Any]:
    """Download media from a WhatsApp message and get the local file path.

    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message

    Returns:
        A dictionary containing success status, a status message, and the file path if successful
    """
    file_path = whatsapp_download_media(message_id, chat_jid)

    if file_path:
        return {"success": True, "message": "Media downloaded successfully", "file_path": file_path}
    else:
        return {"success": False, "message": "Failed to download media"}


@mcp.tool()
def search_messages(
    query: str,
    chat_jid: str | None = None,
    after: str | None = None,
    before: str | None = None,
    limit: int = 30,
) -> list[dict[str, Any]]:
    """Full-text search across every message in the local DB. Powered by
    SQLite FTS5 — typically <100ms across 10s of thousands of messages.

    The query supports FTS5 syntax:
      - plain words: aws cost          → matches messages with both
      - phrase:     "aws cost"         → exact phrase
      - prefix:     sage*              → matches SageMaker, sagemaker etc.
      - boolean:    aws AND credit
      - NEAR:       NEAR(aws credit, 10)  → both words within 10 tokens
    Diacritics are stripped at index time (Muñoz matches "munoz").

    Args:
        query: FTS5 query string
        chat_jid: Optional — restrict to one chat
        after, before: ISO-8601 timestamps to bound the time window
        limit: max rows returned (default 30, sorted by bm25 relevance)

    Returns:
        List of matching messages with chat name + sender, ranked by relevance.
    """
    from whatsapp import search_messages as _search
    return _search(query=query, chat_jid=chat_jid, after=after, before=before, limit=limit)


@mcp.tool()
def catch_up(
    hours: int = 12,
    awaiting_my_reply: bool = False,
    include_groups: bool = True,
    include_dms: bool = True,
    limit_chats: int = 30,
) -> dict[str, Any]:
    """Per-chat activity rollup over the last N hours — designed for inbox
    triage when you've been away.

    Returns chat name, message count, unique senders, last message preview
    (sender + text) for each active chat. Sorted by recency.

    Args:
        hours: lookback window (default 12)
        awaiting_my_reply: True → only chats where the last message wasn't
                           yours (still waiting for your reply). The DB
                           doesn't track WhatsApp's actual read state, so
                           this is the closest available semantic.
        include_groups: include @g.us JIDs
        include_dms: include 1:1 JIDs (@s.whatsapp.net / @lid)
        limit_chats: cap at this many chats (newest activity first)

    Returns:
        {window_hours, since, total_chats, total_messages, chats: [...]}
        On error: same shape with chats=[] and an `error` key.
    """
    from whatsapp import catch_up as _catch_up
    return _catch_up(
        hours=hours,
        awaiting_my_reply=awaiting_my_reply,
        include_groups=include_groups,
        include_dms=include_dms,
        limit_chats=limit_chats,
    )


@mcp.tool()
def transcribe_audio(
    message_id: str,
    chat_jid: str,
    provider: str | None = None,
    language: str = "en",
) -> dict[str, Any]:
    """Transcribe a voice / audio message to text.

    Auto-picks provider based on env (priority: explicit arg →
    WHATSAPP_TRANSCRIBE_PROVIDER env → OPENAI_API_KEY → SARVAM_API_KEY).
    Sarvam saarika handles Hindi / code-switched Hinglish well; OpenAI
    Whisper is broader.

    Args:
        message_id, chat_jid: identify the voice message in the local DB
        provider: 'openai' or 'sarvam' (overrides auto-pick)
        language: ISO code — 'en' (default), 'hi', 'hi-IN', 'en-IN'

    Returns:
        {'success': bool, 'text': '...', 'provider': '...', 'audio_path': '...'}
    """
    from whatsapp import transcribe_audio as _transcribe
    return _transcribe(message_id=message_id, chat_jid=chat_jid, provider=provider, language=language)


@mcp.tool()
def request_history(chat_jid: str, count: int = 100) -> dict[str, Any]:
    """Request older messages from the WhatsApp server for a chat. The
    bridge handles the async response — older messages will appear in
    subsequent list_messages calls within seconds.

    Args:
        chat_jid: e.g. '120363420428178043@g.us'
        count: how many older messages to fetch (1-500, default 100)

    Returns:
        {'success': bool, 'message': '...'}
    """
    from whatsapp import request_history as _request_history
    return _request_history(chat_jid=chat_jid, count=count)


def shutdown_handler(signum, frame):
    """Handle shutdown signals gracefully to prevent zombie processes."""
    sys.exit(0)


if __name__ == "__main__":
    # Register signal handlers for clean shutdown
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Initialize and run the server
    mcp.run(transport="stdio")
