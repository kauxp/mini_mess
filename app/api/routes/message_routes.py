from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from uuid import uuid4, UUID
from datetime import datetime

router = APIRouter()


class SendMessageRequest(BaseModel):
    conversation_id: UUID
    sender_id: UUID
    message_text: str
    participant_ids: Optional[List[UUID]] = None

class MessageResponse(BaseModel):
    message_id: UUID
    conversation_id: UUID
    sender_id: UUID
    message_text: str
    message_timestamp: datetime

cluster = Cluster(contact_points=["127.0.0.1"])
session = cluster.connect("messenger_app")


@router.post("/api/messages/", response_model=MessageResponse)
def send_message(req: SendMessageRequest):
    """
    Send a message in a conversation. Also updates the user_conversations table 
    for each participant.
    """
    
    message_id = uuid4()
    message_timestamp = datetime.utcnow()

    insert_query = """
    INSERT INTO messages_by_conversation (
        conversation_id, message_timestamp, message_id, sender_id, message_text
    ) VALUES (%s, %s, %s, %s, %s)
    """
    session.execute(
        insert_query,
        [
            req.conversation_id,
            message_timestamp,
            message_id,
            req.sender_id,
            req.message_text
        ]
    )

    if not req.participant_ids:
        req.participant_ids = [req.sender_id]

    for user_id in req.participant_ids:
        update_query = """
        INSERT INTO user_conversations (
            user_id, last_activity, conversation_id, participant_ids, last_message_preview
        ) VALUES (%s, %s, %s, %s, %s)
        """
        last_message_preview = req.message_text[:100] 
        session.execute(
            update_query,
            [
                user_id,
                message_timestamp,
                req.conversation_id,
                set(req.participant_ids),
                last_message_preview
            ]
        )

    return MessageResponse(
        message_id=message_id,
        conversation_id=req.conversation_id,
        sender_id=req.sender_id,
        message_text=req.message_text,
        message_timestamp=message_timestamp
    )


@router.get("/api/messages/conversation/{conversation_id}", response_model=List[MessageResponse])
def get_messages_in_conversation(conversation_id: UUID):
    """
    Retrieves all messages in a conversation (or you might limit/paginate).
    Ordered by message_timestamp DESC from Cassandra schema.
    """
    select_query = """
    SELECT conversation_id, message_timestamp, message_id, sender_id, message_text
    FROM messages_by_conversation
    WHERE conversation_id = %s
    ORDER BY message_timestamp DESC
    """
    rows = session.execute(select_query, [conversation_id])
    
    messages = []
    for row in rows:
        messages.append(MessageResponse(
            conversation_id=row.conversation_id,
            message_timestamp=row.message_timestamp,
            message_id=row.message_id,
            sender_id=row.sender_id,
            message_text=row.message_text
        ))
    return messages


@router.get("/api/messages/conversation/{conversation_id}/before", response_model=List[MessageResponse])
def get_messages_before_timestamp(conversation_id: UUID, timestamp: datetime):
    """
    Retrieves messages before a given timestamp. 
    Uses message_timestamp < <provided timestamp> in the WHERE clause.
    """
    select_query = """
    SELECT conversation_id, message_timestamp, message_id, sender_id, message_text
    FROM messages_by_conversation
    WHERE conversation_id = %s
      AND message_timestamp < %s
    ORDER BY message_timestamp DESC
    """
    rows = session.execute(select_query, [conversation_id, timestamp])
    
    messages = []
    for row in rows:
        messages.append(MessageResponse(
            conversation_id=row.conversation_id,
            message_timestamp=row.message_timestamp,
            message_id=row.message_id,
            sender_id=row.sender_id,
            message_text=row.message_text
        ))
    return messages
