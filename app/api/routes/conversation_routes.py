from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from uuid import UUID
from datetime import datetime

router = APIRouter()

class ConversationResponse(BaseModel):
    user_id: UUID
    last_activity: datetime
    conversation_id: UUID
    participant_ids: List[UUID]
    last_message_preview: str

cluster = Cluster(contact_points=["127.0.0.1"])
session = cluster.connect("messenger_app")


@router.get("/api/conversations/user/{user_id}", response_model=List[ConversationResponse])
def get_conversations_for_user(user_id: UUID):
    """
    Get all conversations for a given user, ordered by most recent activity (DESC).
    """
    select_query = """
    SELECT user_id, last_activity, conversation_id, participant_ids, last_message_preview
    FROM user_conversations
    WHERE user_id = %s
    ORDER BY last_activity DESC
    """
    rows = session.execute(select_query, [user_id])

    conversations = []
    for row in rows:
        participant_list = list(row.participant_ids) if row.participant_ids else []
        conversations.append(ConversationResponse(
            user_id=row.user_id,
            last_activity=row.last_activity,
            conversation_id=row.conversation_id,
            participant_ids=participant_list,
            last_message_preview=row.last_message_preview
        ))
    return conversations
