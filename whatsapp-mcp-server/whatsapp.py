import json
import os
import os.path
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import requests

import audio

# Configuration via environment variables with sensible defaults
MESSAGES_DB_PATH = os.getenv(
    "WHATSAPP_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "whatsapp-bridge", "store", "messages.db"),
)
WHATSAPP_API_BASE_URL = os.getenv("WHATSAPP_API_URL", "http://localhost:8080/api")


@dataclass
class Message:
    timestamp: datetime
    sender: str
    content: str
    is_from_me: bool
    chat_jid: str
    id: str
    chat_name: str | None = None
    media_type: str | None = None


@dataclass
class Chat:
    jid: str
    name: str | None
    last_message_time: datetime | None
    last_message: str | None = None
    last_sender: str | None = None
    last_is_from_me: bool | None = None

    @property
    def is_group(self) -> bool:
        """Determine if chat is a group based on JID pattern."""
        return self.jid.endswith("@g.us")


@dataclass
class Contact:
    phone_number: str
    name: str | None
    jid: str


@dataclass
class MessageContext:
    message: Message
    before: list[Message]
    after: list[Message]


def msg_to_dict(message: Message, include_sender_name: bool = True) -> dict[str, Any]:
    """Convert a Message dataclass to a dictionary for JSON serialization."""
    # Extract phone number from JID (e.g., "1234567890@s.whatsapp.net" -> "1234567890")
    sender_phone = message.sender.split("@")[0] if "@" in message.sender else message.sender

    sender_name = None
    sender_display = None
    if include_sender_name:
        if message.is_from_me:
            sender_name = "Me"
            sender_display = "Me"
        else:
            resolved_name = get_sender_name(message.sender)
            # Check if we got an actual name (not just the JID back)
            if resolved_name and resolved_name != message.sender and resolved_name != sender_phone:
                sender_name = resolved_name
                sender_display = f"{resolved_name} ({sender_phone})"
            else:
                sender_name = sender_phone
                sender_display = sender_phone

    return {
        "id": message.id,
        "timestamp": message.timestamp.isoformat(),
        "sender_jid": message.sender,
        "sender_phone": sender_phone,
        "sender_name": sender_name,
        "sender_display": sender_display,  # "Name (phone)" or just phone if no name
        "content": message.content,
        "is_from_me": message.is_from_me,
        "chat_jid": message.chat_jid,
        "chat_name": message.chat_name,
        "media_type": message.media_type,
    }


def chat_to_dict(chat: "Chat") -> dict[str, Any]:
    """Convert a Chat dataclass to a dictionary for JSON serialization."""
    return {
        "jid": chat.jid,
        "name": chat.name,
        "is_group": chat.is_group,
        "last_message_time": chat.last_message_time.isoformat() if chat.last_message_time else None,
        "last_message": chat.last_message,
        "last_sender": chat.last_sender,
        "last_is_from_me": chat.last_is_from_me,
    }


def contact_to_dict(contact: "Contact") -> dict[str, Any]:
    """Convert a Contact dataclass to a dictionary for JSON serialization."""
    return {"phone_number": contact.phone_number, "name": contact.name, "jid": contact.jid}


def get_sender_name(sender_jid: str) -> str:
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        # First try matching by exact JID
        cursor.execute(
            """
            SELECT name
            FROM chats
            WHERE jid = ?
            LIMIT 1
        """,
            (sender_jid,),
        )

        result = cursor.fetchone()

        # If no result, try looking for the number within JIDs
        if not result:
            # Extract the phone number part if it's a JID
            if "@" in sender_jid:
                phone_part = sender_jid.split("@")[0]
            else:
                phone_part = sender_jid

            cursor.execute(
                """
                SELECT name
                FROM chats
                WHERE jid LIKE ?
                LIMIT 1
            """,
                (f"%{phone_part}%",),
            )

            result = cursor.fetchone()

        if result and result[0]:
            return result[0]
        else:
            return sender_jid

    except sqlite3.Error as e:
        print(f"Database error while getting sender name: {e}")
        return sender_jid
    finally:
        if "conn" in locals():
            conn.close()


def format_message(message: Message, show_chat_info: bool = True) -> None:
    """Print a single message with consistent formatting."""
    output = ""

    if show_chat_info and message.chat_name:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] Chat: {message.chat_name} "
    else:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] "

    content_prefix = ""
    if hasattr(message, "media_type") and message.media_type:
        content_prefix = f"[{message.media_type} - Message ID: {message.id} - Chat JID: {message.chat_jid}] "

    try:
        sender_name = get_sender_name(message.sender) if not message.is_from_me else "Me"
        output += f"From: {sender_name}: {content_prefix}{message.content}\n"
    except Exception as e:
        print(f"Error formatting message: {e}")
    return output


def format_messages_list(messages: list[Message], show_chat_info: bool = True) -> None:
    output = ""
    if not messages:
        output += "No messages to display."
        return output

    for message in messages:
        output += format_message(message, show_chat_info)
    return output


def list_messages(
    after: str | None = None,
    before: str | None = None,
    sender_phone_number: str | None = None,
    chat_jid: str | None = None,
    query: str | None = None,
    limit: int = 20,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1,
    sort_by: str = "newest",
) -> list[dict[str, Any]]:
    """Get messages matching the specified criteria with optional context.

    Args:
        after: Optional ISO-8601 formatted string to only return messages after this date
        before: Optional ISO-8601 formatted string to only return messages before this date
        sender_phone_number: Optional phone number to filter messages by sender
        chat_jid: Optional chat JID to filter messages by chat
        query: Optional search term to filter messages by content
        limit: Maximum number of messages to return (default 20)
        page: Page number for pagination (default 0)
        include_context: Whether to include messages before and after matches (default True)
        context_before: Number of messages to include before each match (default 1)
        context_after: Number of messages to include after each match (default 1)
        sort_by: Sort order - "newest" (default) or "oldest" for chronological ordering

    Returns:
        List of message dictionaries with id, timestamp, sender, content, etc.
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        # Build base query
        query_parts = [
            "SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type FROM messages"
        ]
        query_parts.append("JOIN chats ON messages.chat_jid = chats.jid")
        where_clauses = []
        params = []

        # Add filters
        if after:
            try:
                after = datetime.fromisoformat(after)
            except ValueError:
                raise ValueError(f"Invalid date format for 'after': {after}. Please use ISO-8601 format.")

            where_clauses.append("messages.timestamp > ?")
            params.append(after)

        if before:
            try:
                before = datetime.fromisoformat(before)
            except ValueError:
                raise ValueError(f"Invalid date format for 'before': {before}. Please use ISO-8601 format.")

            where_clauses.append("messages.timestamp < ?")
            params.append(before)

        if sender_phone_number:
            where_clauses.append("messages.sender = ?")
            params.append(sender_phone_number)

        if chat_jid:
            where_clauses.append("messages.chat_jid = ?")
            params.append(chat_jid)

        if query:
            where_clauses.append("LOWER(messages.content) LIKE LOWER(?)")
            params.append(f"%{query}%")

        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))

        # Add sorting and pagination
        offset = page * limit
        order = "DESC" if sort_by == "newest" else "ASC"
        query_parts.append(f"ORDER BY messages.timestamp {order}")
        query_parts.append("LIMIT ? OFFSET ?")
        params.extend([limit, offset])

        cursor.execute(" ".join(query_parts), tuple(params))
        messages = cursor.fetchall()

        result = []
        for msg in messages:
            message = Message(
                timestamp=datetime.fromisoformat(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7],
            )
            result.append(message)

        if include_context and result:
            # Add context for each message, deduplicated by message ID
            seen_ids = set()
            messages_with_context = []
            for msg in result:
                context = get_message_context(msg.id, context_before, context_after)
                for ctx_msg in context.before:
                    if ctx_msg.id not in seen_ids:
                        seen_ids.add(ctx_msg.id)
                        messages_with_context.append(ctx_msg)
                if context.message.id not in seen_ids:
                    seen_ids.add(context.message.id)
                    messages_with_context.append(context.message)
                for ctx_msg in context.after:
                    if ctx_msg.id not in seen_ids:
                        seen_ids.add(ctx_msg.id)
                        messages_with_context.append(ctx_msg)

            return [msg_to_dict(msg) for msg in messages_with_context]

        # Return messages without context
        return [msg_to_dict(msg) for msg in result]

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if "conn" in locals():
            conn.close()


def get_message_context(message_id: str, before: int = 5, after: int = 5) -> MessageContext:
    """Get context around a specific message."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        # Get the target message first
        cursor.execute(
            """
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.chat_jid, messages.media_type
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.id = ?
        """,
            (message_id,),
        )
        msg_data = cursor.fetchone()

        if not msg_data:
            raise ValueError(f"Message with ID {message_id} not found")

        target_message = Message(
            timestamp=datetime.fromisoformat(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6],
            media_type=msg_data[8],
        )

        # Get messages before
        cursor.execute(
            """
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = ? AND messages.timestamp < ?
            ORDER BY messages.timestamp DESC
            LIMIT ?
        """,
            (msg_data[7], msg_data[0], before),
        )

        before_messages = []
        for msg in cursor.fetchall():
            before_messages.append(
                Message(
                    timestamp=datetime.fromisoformat(msg[0]),
                    sender=msg[1],
                    chat_name=msg[2],
                    content=msg[3],
                    is_from_me=msg[4],
                    chat_jid=msg[5],
                    id=msg[6],
                    media_type=msg[7],
                )
            )

        # Get messages after
        cursor.execute(
            """
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = ? AND messages.timestamp > ?
            ORDER BY messages.timestamp ASC
            LIMIT ?
        """,
            (msg_data[7], msg_data[0], after),
        )

        after_messages = []
        for msg in cursor.fetchall():
            after_messages.append(
                Message(
                    timestamp=datetime.fromisoformat(msg[0]),
                    sender=msg[1],
                    chat_name=msg[2],
                    content=msg[3],
                    is_from_me=msg[4],
                    chat_jid=msg[5],
                    id=msg[6],
                    media_type=msg[7],
                )
            )

        return MessageContext(message=target_message, before=before_messages, after=after_messages)

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    finally:
        if "conn" in locals():
            conn.close()


def list_chats(
    query: str | None = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active",
) -> list[dict[str, Any]]:
    """Get chats matching the specified criteria.

    Returns:
        List of chat dictionaries with jid, name, is_group, last_message, etc.
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        # Build base query
        query_parts = [
            """
            SELECT
                chats.jid,
                chats.name,
                chats.last_message_time,
                messages.content as last_message,
                messages.sender as last_sender,
                messages.is_from_me as last_is_from_me
            FROM chats
        """
        ]

        if include_last_message:
            query_parts.append("""
                LEFT JOIN messages ON chats.jid = messages.chat_jid
                AND chats.last_message_time = messages.timestamp
            """)

        where_clauses = []
        params = []

        if query:
            where_clauses.append("(LOWER(chats.name) LIKE LOWER(?) OR chats.jid LIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])

        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))

        # Add sorting
        order_by = "chats.last_message_time DESC" if sort_by == "last_active" else "chats.name"
        query_parts.append(f"ORDER BY {order_by}")

        # Add pagination
        offset = (page) * limit
        query_parts.append("LIMIT ? OFFSET ?")
        params.extend([limit, offset])

        cursor.execute(" ".join(query_parts), tuple(params))
        chats = cursor.fetchall()

        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5],
            )
            result.append(chat_to_dict(chat))

        return result

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if "conn" in locals():
            conn.close()


def search_contacts(query: str) -> list[dict[str, Any]]:
    """Search contacts by name or phone number."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        # Split query into characters to support partial matching
        search_pattern = "%" + query + "%"

        cursor.execute(
            """
            SELECT DISTINCT
                jid,
                name
            FROM chats
            WHERE
                (LOWER(name) LIKE LOWER(?) OR LOWER(jid) LIKE LOWER(?))
                AND jid NOT LIKE '%@g.us'
            ORDER BY name, jid
            LIMIT 50
        """,
            (search_pattern, search_pattern),
        )

        contacts = cursor.fetchall()

        result = []
        for contact_data in contacts:
            contact = Contact(phone_number=contact_data[0].split("@")[0], name=contact_data[1], jid=contact_data[0])
            result.append(contact_to_dict(contact))

        return result

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if "conn" in locals():
            conn.close()


def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> list[dict[str, Any]]:
    """Get all chats involving the contact.

    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT DISTINCT
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
            JOIN messages m ON c.jid = m.chat_jid
            WHERE m.sender = ? OR c.jid = ?
            ORDER BY c.last_message_time DESC
            LIMIT ? OFFSET ?
        """,
            (jid, jid, limit, page * limit),
        )

        chats = cursor.fetchall()

        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5],
            )
            result.append(chat_to_dict(chat))

        return result

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if "conn" in locals():
            conn.close()


def get_last_interaction(jid: str) -> dict[str, Any] | None:
    """Get most recent message involving the contact.

    Args:
        jid: The JID of the contact to search for

    Returns:
        Message dictionary or None if no messages found
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                m.timestamp,
                m.sender,
                c.name,
                m.content,
                m.is_from_me,
                c.jid,
                m.id,
                m.media_type
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.sender = ? OR c.jid = ?
            ORDER BY m.timestamp DESC
            LIMIT 1
        """,
            (jid, jid),
        )

        msg_data = cursor.fetchone()

        if not msg_data:
            return None

        message = Message(
            timestamp=datetime.fromisoformat(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6],
            media_type=msg_data[7],
        )

        return msg_to_dict(message)

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if "conn" in locals():
            conn.close()


def get_chat(chat_jid: str, include_last_message: bool = True) -> dict[str, Any] | None:
    """Get chat metadata by JID.

    Returns:
        Chat dictionary or None if not found
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        query = """
            SELECT
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
        """

        if include_last_message:
            query += """
                LEFT JOIN messages m ON c.jid = m.chat_jid
                AND c.last_message_time = m.timestamp
            """

        query += " WHERE c.jid = ?"

        cursor.execute(query, (chat_jid,))
        chat_data = cursor.fetchone()

        if not chat_data:
            return None

        chat = Chat(
            jid=chat_data[0],
            name=chat_data[1],
            last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5],
        )
        return chat_to_dict(chat)

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if "conn" in locals():
            conn.close()


def get_direct_chat_by_contact(sender_phone_number: str) -> dict[str, Any] | None:
    """Get chat metadata by sender phone number."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
            LEFT JOIN messages m ON c.jid = m.chat_jid
                AND c.last_message_time = m.timestamp
            WHERE c.jid LIKE ? AND c.jid NOT LIKE '%@g.us'
            LIMIT 1
        """,
            (f"%{sender_phone_number}%",),
        )

        chat_data = cursor.fetchone()

        if not chat_data:
            return None

        chat = Chat(
            jid=chat_data[0],
            name=chat_data[1],
            last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5],
        )
        return chat_to_dict(chat)

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if "conn" in locals():
            conn.close()


def send_message(recipient: str, message: str) -> tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"

        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "message": message,
        }

        response = requests.post(url, json=payload)

        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"

    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def _post_bridge(path: str, payload: dict) -> tuple[bool, str, dict]:
    """Helper: POST a JSON payload to the bridge REST API and return (success, message, full_json)."""
    try:
        url = f"{WHATSAPP_API_BASE_URL}{path}"
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", ""), result
        return False, f"HTTP {response.status_code}: {response.text}", {}
    except requests.RequestException as e:
        return False, f"Request error: {e}", {}
    except json.JSONDecodeError:
        return False, "Error parsing response", {}
    except Exception as e:
        return False, f"Unexpected error: {e}", {}


def reply_to_message(
    recipient: str, message: str, reply_to_message_id: str, reply_to_sender: str = ""
) -> tuple[bool, str]:
    ok, msg, _ = _post_bridge(
        "/reply",
        {
            "recipient": recipient,
            "message": message,
            "reply_to_message_id": reply_to_message_id,
            "reply_to_sender": reply_to_sender,
        },
    )
    return ok, msg


def forward_message(recipient: str, message_id: str, source_chat_jid: str) -> tuple[bool, str]:
    ok, msg, _ = _post_bridge(
        "/forward",
        {"recipient": recipient, "message_id": message_id, "source_chat_jid": source_chat_jid},
    )
    return ok, msg


def edit_message(recipient: str, message_id: str, new_message: str) -> tuple[bool, str]:
    ok, msg, _ = _post_bridge("/edit", {"recipient": recipient, "message_id": message_id, "new_message": new_message})
    return ok, msg


def revoke_message(recipient: str, message_id: str, sender: str = "") -> tuple[bool, str]:
    ok, msg, _ = _post_bridge("/revoke", {"recipient": recipient, "message_id": message_id, "sender": sender})
    return ok, msg


def mark_messages_read(
    chat_jid: str, message_ids: list[str], sender_jid: str = "", receipt_type: str = "read"
) -> tuple[bool, str]:
    ok, msg, _ = _post_bridge(
        "/read",
        {
            "chat_jid": chat_jid,
            "sender_jid": sender_jid,
            "message_ids": message_ids,
            "receipt_type": receipt_type,
        },
    )
    return ok, msg


def mark_chat_read(chat_jid: str) -> tuple[bool, str]:
    ok, msg, _ = _post_bridge("/chat_read", {"chat_jid": chat_jid})
    return ok, msg


def get_group_info(group_jid: str) -> dict:
    _, _, full = _post_bridge("/group/info", {"group_jid": group_jid})
    return full


def update_group_participants(group_jid: str, action: str, jids: list[str]) -> tuple[bool, str, list]:
    ok, msg, full = _post_bridge("/group/participants", {"group_jid": group_jid, "action": action, "jids": jids})
    return ok, msg, full.get("participants", [])


def send_location(
    recipient: str, latitude: float, longitude: float, name: str = "", address: str = ""
) -> tuple[bool, str]:
    ok, msg, _ = _post_bridge(
        "/location",
        {
            "recipient": recipient,
            "latitude": latitude,
            "longitude": longitude,
            "name": name,
            "address": address,
        },
    )
    return ok, msg


def send_sticker(recipient: str, sticker_path: str) -> tuple[bool, str]:
    ok, msg, _ = _post_bridge("/sticker", {"recipient": recipient, "sticker_path": sticker_path})
    return ok, msg


def list_recent_calls(limit: int = 50, after: str = "") -> dict:
    _, _, full = _post_bridge("/calls/recent", {"limit": limit, "after": after})
    return full


def react_to_message(recipient: str, message_id: str, emoji: str, from_me: bool = True) -> tuple[bool, str]:
    """Send an emoji reaction to an existing WhatsApp message.

    Args:
        recipient: Phone number (no +) or full JID of the chat the message is in.
        message_id: The ID of the message to react to.
        emoji: The reaction emoji (e.g. "👍"). Pass an empty string to remove a prior reaction.
        from_me: True if the original message was sent by this account (i.e. you sent it).
                 False if the original message was received from someone else.
    """
    try:
        if not recipient:
            return False, "Recipient must be provided"
        if not message_id:
            return False, "message_id must be provided"

        url = f"{WHATSAPP_API_BASE_URL}/react"
        payload = {
            "recipient": recipient,
            "message_id": message_id,
            "emoji": emoji,
            "from_me": from_me,
        }
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def send_file(recipient: str, media_path: str) -> tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"

        if not media_path:
            return False, "Media path must be provided"

        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"

        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {"recipient": recipient, "media_path": media_path}

        response = requests.post(url, json=payload)

        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"

    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def send_audio_message(recipient: str, media_path: str) -> tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"

        if not media_path:
            return False, "Media path must be provided"

        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"

        if not media_path.endswith(".ogg"):
            try:
                media_path = audio.convert_to_opus_ogg_temp(media_path)
            except Exception as e:
                return False, f"Error converting file to opus ogg. You likely need to install ffmpeg: {str(e)}"

        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {"recipient": recipient, "media_path": media_path}

        response = requests.post(url, json=payload)

        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"

    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def download_media(message_id: str, chat_jid: str) -> str | None:
    """Download media from a message and return the local file path.

    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message

    Returns:
        The local file path if download was successful, None otherwise
    """
    try:
        url = f"{WHATSAPP_API_BASE_URL}/download"
        payload = {"message_id": message_id, "chat_jid": chat_jid}

        response = requests.post(url, json=payload)

        if response.status_code == 200:
            result = response.json()
            if result.get("success", False):
                path = result.get("path")
                print(f"Media downloaded successfully: {path}")
                return path
            else:
                print(f"Download failed: {result.get('message', 'Unknown error')}")
                return None
        else:
            print(f"Error: HTTP {response.status_code} - {response.text}")
            return None

    except requests.RequestException as e:
        print(f"Request error: {str(e)}")
        return None
    except json.JSONDecodeError:
        print(f"Error parsing response: {response.text}")
        return None
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return None


# ─── Full-text search via SQLite FTS5 ──────────────────────────────────────
# A virtual FTS5 table (`messages_fts`) is created once on demand and kept in
# sync with `messages` via three triggers (insert / update / delete). The
# first call to `ensure_fts_indexes` backfills the FTS table from any existing
# rows. Subsequent calls are no-ops.
_FTS_READY = False


def ensure_fts_indexes(conn: sqlite3.Connection) -> None:
    """Idempotent — sets up the FTS5 virtual table + triggers + backfill.

    `messages_fts` mirrors `messages` on three columns the user is likely to
    search by: content, chat_jid, sender. Rowid is keyed to the (id, chat_jid)
    pair via a synthetic uuid stored in messages.
    """
    global _FTS_READY
    if _FTS_READY:
        return
    cur = conn.cursor()

    # Check if FTS table already exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='messages_fts'")
    if cur.fetchone() is None:
        # Create the FTS5 virtual table — content-less, references messages rowid.
        cur.execute("""
            CREATE VIRTUAL TABLE messages_fts USING fts5(
                content, sender, chat_jid,
                content='messages',
                content_rowid='rowid',
                tokenize='unicode61 remove_diacritics 2'
            )
        """)
        # Triggers to keep FTS in sync with the messages table.
        cur.execute("""
            CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
              INSERT INTO messages_fts(rowid, content, sender, chat_jid)
              VALUES (new.rowid, new.content, new.sender, new.chat_jid);
            END
        """)
        cur.execute("""
            CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
              INSERT INTO messages_fts(messages_fts, rowid, content, sender, chat_jid)
              VALUES('delete', old.rowid, old.content, old.sender, old.chat_jid);
            END
        """)
        cur.execute("""
            CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
              INSERT INTO messages_fts(messages_fts, rowid, content, sender, chat_jid)
              VALUES('delete', old.rowid, old.content, old.sender, old.chat_jid);
              INSERT INTO messages_fts(rowid, content, sender, chat_jid)
              VALUES (new.rowid, new.content, new.sender, new.chat_jid);
            END
        """)
        # Backfill from existing rows. Single statement, atomic.
        cur.execute("""
            INSERT INTO messages_fts(rowid, content, sender, chat_jid)
            SELECT rowid, content, sender, chat_jid FROM messages
        """)
        conn.commit()
    _FTS_READY = True


def search_messages(
    query: str,
    chat_jid: str | None = None,
    after: str | None = None,
    before: str | None = None,
    limit: int = 30,
) -> list[dict[str, Any]]:
    """Full-text search messages using SQLite FTS5. Sub-second across the
    whole DB. Supports FTS5 syntax: phrase queries ("aws cost"), prefix
    (sage*), boolean (aws AND credit), NEAR (NEAR(aws credit, 10)).

    Args:
        query: FTS5 query string (plain words OK; advanced syntax supported)
        chat_jid: Optional — restrict to a single chat
        after, before: Optional ISO-8601 timestamps to bound the time window
        limit: max rows to return

    Returns:
        List of message dicts ordered by FTS relevance (bm25), most relevant first.
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        conn.row_factory = sqlite3.Row
        ensure_fts_indexes(conn)
        cur = conn.cursor()

        where = ["messages_fts MATCH ?"]
        params: list[Any] = [query]
        if chat_jid:
            where.append("m.chat_jid = ?")
            params.append(chat_jid)
        if after:
            where.append("m.timestamp >= ?")
            params.append(after)
        if before:
            where.append("m.timestamp <= ?")
            params.append(before)

        sql = f"""
            SELECT m.id, m.chat_jid, m.sender, m.content, m.timestamp,
                   m.is_from_me, m.media_type, m.filename,
                   c.name AS chat_name,
                   bm25(messages_fts) AS rank
              FROM messages_fts
              JOIN messages m ON m.rowid = messages_fts.rowid
              LEFT JOIN chats c ON c.jid = m.chat_jid
             WHERE {" AND ".join(where)}
             ORDER BY rank
             LIMIT ?
        """
        params.append(limit)
        cur.execute(sql, params)
        out: list[dict[str, Any]] = []
        for row in cur.fetchall():
            d = dict(row)
            d["timestamp"] = str(d["timestamp"])
            out.append(d)
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─── Catch-up summary ──────────────────────────────────────────────────────
def catch_up(
    hours: int = 12,
    awaiting_my_reply: bool = False,
    include_groups: bool = True,
    include_dms: bool = True,
    limit_chats: int = 30,
) -> dict[str, Any]:
    """Roll up activity across all chats in the last N hours.

    Returns a per-chat summary: chat name, JID, message count, unique senders,
    last message preview. Sorted by recency. Designed for triage — pair it
    with list_messages on chats you want to drill into.

    Args:
        hours: Lookback window (default 12)
        awaiting_my_reply: If True, only show chats where the last message
                           wasn't from me — i.e., still waiting for my reply.
                           WhatsApp's actual read state isn't stored in this
                           DB, so this is the closest available semantic
                           (`last_message.is_from_me = 0`).
        include_groups: Include @g.us JIDs
        include_dms: Include @s.whatsapp.net / @lid 1:1 JIDs
        limit_chats: Cap the chat list (newest activity first)

    Returns:
        {window_hours, since, total_chats, total_messages, chats: [...]}
        On error: same shape with `chats: []` and an `error` key.
    """
    from datetime import datetime, timedelta

    cutoff = (datetime.now(UTC) - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    conn = None
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        chat_clauses = []
        if include_groups:
            chat_clauses.append("chat_jid LIKE '%@g.us'")
        if include_dms:
            chat_clauses.append("chat_jid LIKE '%@s.whatsapp.net' OR chat_jid LIKE '%@lid'")
        chat_filter = "(" + " OR ".join(chat_clauses) + ")" if chat_clauses else "1=1"
        awaiting_filter = "AND l.is_from_me = 0" if awaiting_my_reply else ""

        # Single-query catch_up. chat_activity aggregates per-chat stats over
        # the window; last_msg picks the most recent message per chat via
        # ROW_NUMBER. Pushing awaiting_my_reply into SQL avoids fetching +
        # serialising rows we'd otherwise discard — fewer queries, smaller
        # MCP stdio payload.
        cur.execute(
            f"""
            WITH chat_activity AS (
                SELECT m.chat_jid,
                       COUNT(*) AS message_count,
                       COUNT(DISTINCT m.sender) AS unique_senders,
                       MAX(m.timestamp) AS last_ts,
                       SUM(CASE WHEN m.is_from_me=1 THEN 1 ELSE 0 END) AS my_messages
                  FROM messages m
                 WHERE m.timestamp >= ?
                   AND {chat_filter}
                 GROUP BY m.chat_jid
            ),
            last_msg AS (
                SELECT chat_jid, sender, content, is_from_me, timestamp,
                       ROW_NUMBER() OVER (PARTITION BY chat_jid ORDER BY timestamp DESC) AS rn
                  FROM messages
                 WHERE chat_jid IN (SELECT chat_jid FROM chat_activity)
            )
            SELECT a.chat_jid,
                   c.name AS chat_name,
                   a.message_count,
                   a.unique_senders,
                   a.last_ts,
                   a.my_messages,
                   l.sender AS last_sender,
                   substr(l.content, 1, 100) AS last_preview,
                   l.is_from_me AS last_is_from_me,
                   l.timestamp AS last_timestamp
              FROM chat_activity a
              LEFT JOIN chats c ON c.jid = a.chat_jid
              LEFT JOIN last_msg l ON l.chat_jid = a.chat_jid AND l.rn = 1
             WHERE 1=1 {awaiting_filter}
             ORDER BY a.last_ts DESC
             LIMIT ?
        """,
            (cutoff, limit_chats),
        )

        rows = []
        for r in cur.fetchall():
            row = {
                "chat_jid": r["chat_jid"],
                "chat_name": r["chat_name"],
                "message_count": r["message_count"],
                "unique_senders": r["unique_senders"],
                "last_ts": r["last_ts"],
                "my_messages": r["my_messages"],
            }
            if r["last_sender"] is not None:
                row["last_message"] = {
                    "sender": r["last_sender"],
                    "preview": r["last_preview"],
                    "is_from_me": r["last_is_from_me"],
                    "timestamp": r["last_timestamp"],
                }
            rows.append(row)

        total_msgs = sum(r["message_count"] for r in rows)
        return {
            "window_hours": hours,
            "since": cutoff + "Z",
            "total_chats": len(rows),
            "total_messages": total_msgs,
            "chats": rows,
        }
    except Exception as e:
        print(f"Database error in catch_up: {e}")
        return {
            "window_hours": hours,
            "since": cutoff + "Z",
            "total_chats": 0,
            "total_messages": 0,
            "chats": [],
            "error": str(e),
        }
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ─── Voice transcription ───────────────────────────────────────────────────
def transcribe_audio(
    message_id: str,
    chat_jid: str,
    provider: str | None = None,
    language: str = "en",
) -> dict[str, Any]:
    """Download a voice/audio message and transcribe it.

    Provider resolution (env-driven; first non-empty wins):
      - explicit `provider` arg → 'openai' | 'sarvam'
      - WHATSAPP_TRANSCRIBE_PROVIDER env
      - OPENAI_API_KEY set → openai
      - SARVAM_API_KEY set → sarvam
      - else: error with guidance

    Args:
        message_id, chat_jid: identify the voice message to fetch
        provider: 'openai' or 'sarvam' (optional override)
        language: ISO code; 'en' default; 'hi-IN' for Hindi via Sarvam

    Returns:
        {'success': bool, 'text': str, 'provider': str, 'audio_path': str, ...}
    """
    if not provider:
        provider = os.getenv("WHATSAPP_TRANSCRIBE_PROVIDER")
    if not provider:
        if os.getenv("OPENAI_API_KEY"):
            provider = "openai"
        elif os.getenv("SARVAM_API_KEY"):
            provider = "sarvam"
        else:
            return {
                "success": False,
                "error": "no transcription provider configured. Set OPENAI_API_KEY or SARVAM_API_KEY, or pass provider='openai'/'sarvam'",
            }

    audio_path = download_media(message_id, chat_jid)
    if not audio_path or not os.path.isfile(audio_path):
        return {"success": False, "error": f"could not download media for {message_id}"}

    try:
        if provider == "openai":
            return _transcribe_openai(audio_path, language)
        elif provider == "sarvam":
            return _transcribe_sarvam(audio_path, language)
        else:
            return {"success": False, "error": f"unknown provider '{provider}'"}
    except Exception as e:
        return {"success": False, "error": f"transcription failed: {e}", "audio_path": audio_path, "provider": provider}


def _transcribe_openai(audio_path: str, language: str) -> dict[str, Any]:
    """Use OpenAI Whisper API. Cheap (~$0.006/min) and reliable for Indic+EN."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {"success": False, "error": "OPENAI_API_KEY not set"}
    with open(audio_path, "rb") as f:
        resp = requests.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {api_key}"},
            files={"file": (os.path.basename(audio_path), f, "audio/ogg")},
            data={"model": "whisper-1", "language": language[:2]},
            timeout=120,
        )
    if resp.status_code != 200:
        return {
            "success": False,
            "error": f"openai whisper {resp.status_code}: {resp.text[:200]}",
            "audio_path": audio_path,
        }
    data = resp.json()
    return {"success": True, "text": data.get("text", ""), "provider": "openai-whisper", "audio_path": audio_path}


def _transcribe_sarvam(audio_path: str, language: str) -> dict[str, Any]:
    """Use Sarvam's saarika ASR (better for Hindi / Indic languages, including
    Hinglish code-switched audio. KC already pays Sarvam — no extra vendor)."""
    api_key = os.getenv("SARVAM_API_KEY")
    if not api_key:
        return {"success": False, "error": "SARVAM_API_KEY not set"}
    # Sarvam expects 'hi-IN', 'en-IN', etc. Default unknown lang to en-IN.
    lang_code = language if "-" in language else f"{language}-IN"
    with open(audio_path, "rb") as f:
        resp = requests.post(
            "https://api.sarvam.ai/speech-to-text",
            headers={"api-subscription-key": api_key},
            files={"file": (os.path.basename(audio_path), f, "audio/ogg")},
            data={"language_code": lang_code, "model": "saarika:v2.5"},
            timeout=120,
        )
    if resp.status_code != 200:
        return {"success": False, "error": f"sarvam {resp.status_code}: {resp.text[:200]}", "audio_path": audio_path}
    data = resp.json()
    return {
        "success": True,
        "text": data.get("transcript", ""),
        "provider": "sarvam-saarika:v2.5",
        "audio_path": audio_path,
    }


# ─── History sync request (bridge passthrough) ─────────────────────────────
def request_history(chat_jid: str, count: int = 100) -> dict[str, Any]:
    """Ask the WhatsApp server for older messages in a chat. Newly delivered
    messages arrive via the bridge's HistorySync event handler and land in
    the local DB; this call just kicks off the request — poll list_messages
    a few seconds later to see them.

    Args:
        chat_jid: e.g. '120363420428178043@g.us'
        count: how many older messages to fetch (default 100, max 500)

    Returns:
        {'success': bool, 'message': str}
    """
    try:
        url = f"{WHATSAPP_API_BASE_URL}/history_sync"
        resp = requests.post(url, json={"chat_jid": chat_jid, "count": min(max(1, count), 500)}, timeout=30)
        return (
            resp.json()
            if resp.status_code == 200
            else {
                "success": False,
                "message": f"bridge returned {resp.status_code}: {resp.text[:200]}",
            }
        )
    except requests.exceptions.RequestException as e:
        return {"success": False, "message": f"bridge call failed: {e}"}
