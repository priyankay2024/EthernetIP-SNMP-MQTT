"""
Migration script to handle the removal of topic_prefix field and update of publish_topic
Run this script BEFORE starting the application with the new changes
"""

import sqlite3
import os
from datetime import datetime

def migrate_database():
    """Migrate the database to handle topic_prefix to publish_topic changes"""
    db_path = os.path.join(os.path.dirname(__file__), 'instance', 'bridge_logic.db')
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if mqtt_config table exists and has the old structure
        cursor.execute("PRAGMA table_info(mqtt_config)")
        columns = [column[1] for column in cursor.fetchall()]
        
        has_topic_prefix = 'topic_prefix' in columns
        has_publish_topic = 'publish_topic' in columns
        
        print(f"Current table structure: {columns}")
        print(f"Has topic_prefix: {has_topic_prefix}")
        print(f"Has publish_topic: {has_publish_topic}")
        
        if has_topic_prefix:
            # Step 1: Update publish_topic for records where it's null/empty using topic_prefix
            if has_publish_topic:
                cursor.execute("""
                    UPDATE mqtt_config 
                    SET publish_topic = COALESCE(publish_topic, topic_prefix, 'sensors/data')
                    WHERE publish_topic IS NULL OR publish_topic = ''
                """)
                print(f"Updated {cursor.rowcount} records with publish_topic from topic_prefix")
            else:
                # Add publish_topic column if it doesn't exist
                cursor.execute("ALTER TABLE mqtt_config ADD COLUMN publish_topic VARCHAR(255)")
                print("Added publish_topic column")
                
                # Set publish_topic to topic_prefix for all records
                cursor.execute("UPDATE mqtt_config SET publish_topic = COALESCE(topic_prefix, 'sensors/data')")
                print(f"Set publish_topic for {cursor.rowcount} records")
            
            # Step 2: Create new table without topic_prefix
            cursor.execute("""
                CREATE TABLE mqtt_config_new (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(100) NOT NULL DEFAULT 'Default',
                    broker VARCHAR(255) NOT NULL,
                    port INTEGER DEFAULT 1883,
                    username VARCHAR(100),
                    password VARCHAR(255),
                    publish_format VARCHAR(20) DEFAULT 'json',
                    use_tls BOOLEAN DEFAULT 0,
                    publish_topic VARCHAR(255) NOT NULL,
                    subscribe_topic VARCHAR(255),
                    publish_interval INTEGER DEFAULT 5,
                    enabled BOOLEAN DEFAULT 1,
                    created_at DATETIME,
                    updated_at DATETIME
                )
            """)
            print("Created new mqtt_config table structure")
            
            # Step 3: Copy data to new table
            cursor.execute("""
                INSERT INTO mqtt_config_new 
                (id, name, broker, port, username, password, publish_format, use_tls, 
                 publish_topic, subscribe_topic, publish_interval, enabled, created_at, updated_at)
                SELECT 
                    id, name, broker, port, username, password, publish_format, use_tls,
                    COALESCE(publish_topic, topic_prefix, 'sensors/data') as publish_topic,
                    subscribe_topic, publish_interval, enabled, created_at, updated_at
                FROM mqtt_config
            """)
            print(f"Copied {cursor.rowcount} records to new table")
            
            # Step 4: Replace old table with new table
            cursor.execute("DROP TABLE mqtt_config")
            cursor.execute("ALTER TABLE mqtt_config_new RENAME TO mqtt_config")
            print("Replaced old table with new structure")
            
        else:
            print("topic_prefix column not found - database may already be migrated")
            
            # Ensure publish_topic is not null
            cursor.execute("SELECT COUNT(*) FROM mqtt_config WHERE publish_topic IS NULL OR publish_topic = ''")
            null_count = cursor.fetchone()[0]
            
            if null_count > 0:
                cursor.execute("UPDATE mqtt_config SET publish_topic = 'sensors/data' WHERE publish_topic IS NULL OR publish_topic = ''")
                print(f"Fixed {cursor.rowcount} records with empty publish_topic")
        
        conn.commit()
        conn.close()
        
        print("Database migration completed successfully!")
        return True
        
    except Exception as e:
        print(f"Migration failed: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

if __name__ == "__main__":
    print("Starting database migration...")
    success = migrate_database()
    if success:
        print("Migration completed successfully!")
        print("You can now start the application with the new changes.")
    else:
        print("Migration failed! Please check the errors above.")