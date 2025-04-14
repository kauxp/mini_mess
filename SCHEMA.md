## Cassandra Schema Design for Messenger App

The goal of this schema is to efficiently handle these functionalities:

- Sending messages between users.
- Fetching user conversations ordered by recent activity.
- Fetching all messages in a conversation.
- Fetching messages before a specific timestamp (pagination).

## Key Considerations:

- **Cassandra** is optimized for read operations; therefore, data duplication and denormalization are necessary to ensure efficient queries.
- Tables are designed around query patterns, optimizing partition and clustering keys.

---

## Schema Design

### 1. Table: `messages_by_conversation`

Stores all messages within each conversation, sorted by timestamp (descending) to enable quick access to recent messages and pagination.

```sql
CREATE TABLE IF NOT EXISTS messages_by_conversation (
    conversation_id UUID,
    message_timestamp TIMESTAMP,
    message_id UUID,
    sender_id UUID,
    message_text TEXT,
    PRIMARY KEY (conversation_id, message_timestamp, message_id)
) WITH CLUSTERING ORDER BY (message_timestamp DESC, message_id ASC);
```

**Query Supported**:

- Fetch all messages for a conversation.
- Fetch messages before a specific timestamp for pagination purposes.

---

### 2. Table: `user_conversations`

Stores a summary of all conversations a user participates in, ordered by recent activity.

```sql
CREATE TABLE IF NOT EXISTS user_conversations (
    user_id UUID,
    last_activity TIMESTAMP,
    conversation_id UUID,
    participant_ids SET<UUID>,
    last_message_preview TEXT,
    PRIMARY KEY (user_id, last_activity, conversation_id)
) WITH CLUSTERING ORDER BY (last_activity DESC);
```

**Query Patterns Supported**:

- Fetch conversations ordered by the most recent activity.
