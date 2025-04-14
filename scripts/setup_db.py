"""
Script to initialize Cassandra keyspace and tables for the Messenger application.
"""
import os
import time
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

def create_tables():
    # Connect to your Cassandra cluster (change contact_points as needed)
    cluster = Cluster(contact_points=[CASSANDRA_HOST])
    session = cluster.connect()

    # Create or use an existing keyspace (update replication settings as needed)
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS messenger_app
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)

    # Switch to the keyspace
    session.set_keyspace('messenger_app')

    # Create messages_by_conversation table
    create_messages_table = """
    CREATE TABLE IF NOT EXISTS messages_by_conversation (
        conversation_id UUID,
        message_timestamp TIMESTAMP,
        message_id UUID,
        sender_id UUID,
        message_text TEXT,
        PRIMARY KEY (conversation_id, message_timestamp, message_id)
    )
    WITH CLUSTERING ORDER BY (message_timestamp DESC, message_id ASC);
    """
    session.execute(SimpleStatement(create_messages_table))

    # Create user_conversations table
    create_user_conversations_table = """
    CREATE TABLE IF NOT EXISTS user_conversations (
        user_id UUID,
        last_activity TIMESTAMP,
        conversation_id UUID,
        participant_ids SET<UUID>,
        last_message_preview TEXT,
        PRIMARY KEY (user_id, last_activity, conversation_id)
    )
    WITH CLUSTERING ORDER BY (last_activity DESC);
    """
    session.execute(SimpleStatement(create_user_conversations_table))

    # (Optional) Confirm creation
    print("Tables created successfully!")

    # Close the session and cluster
    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    create_tables()
