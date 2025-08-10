# CHANGE LOG
# File: /var/www/memory_system/git_mirror_exporter.py
# Document Type: Python Utility Script
# Purpose: Export public memory conversations to Git mirror for AI assistant access
# Main App: memory_app.py
# Dependencies: mysql.connector, json, os, subprocess, pathlib
# Context: Creates static JSON files in Git repo for reliable AI access via raw URLs
# Mirror Strategy: Public redacted content only, private stays in MariaDB
# Version History:
# 2025-08-10 v1.0 - Initial creation following development standards

import os
import json
import hashlib
import subprocess
import traceback
import mysql.connector
from mysql.connector import Error as MySQLError
from datetime import datetime
from pathlib import Path

# Configuration
REPO_DIR = "/srv/memory-git"
CATALOG_DIR = f"{REPO_DIR}/catalog"
SHARD_TAG_DIR = f"{CATALOG_DIR}/shards/by_tag"
SHARD_DATE_DIR = f"{CATALOG_DIR}/shards/by_date"
CHUNK_DIR = f"{REPO_DIR}/chunks"
META_DIR = f"{REPO_DIR}/meta"
CHUNK_SIZE = 2000
PUBLIC_TAGS = {"memory", "breakthrough", "historic", "system", "development"}

# Database configuration (same as main app)
DB_CONFIG = {
    'host': 'localhost',
    'database': 'memory_system',
    'user': 'memory_user',
    'password': 'memory_pass_2025'
}

def get_db_connection():
    """Get database connection with error handling"""
    try:
        print(f"DEBUG: get_db_connection - Connecting to MariaDB at {datetime.now()}")
        conn = mysql.connector.connect(**DB_CONFIG)
        
        if not conn.is_connected():
            print("ERROR: get_db_connection - Failed to connect to MariaDB")
            return None
            
        print("DEBUG: get_db_connection - Connected to MariaDB successfully")
        return conn
        
    except MySQLError as e:
        print(f"ERROR: get_db_connection - MySQL error: {e}")
        return None
    except Exception as e:
        print(f"ERROR: get_db_connection - Unexpected error: {e}")
        return None

def ensure_directory(path):
    """Create directory if it doesn't exist"""
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
        print(f"DEBUG: ensure_directory - Directory ready: {path}")
        return True
    except Exception as e:
        print(f"ERROR: ensure_directory - Failed to create {path}: {e}")
        return False

def redact_content(text):
    """Remove sensitive information from content"""
    try:
        print("DEBUG: redact_content - Starting content redaction")
        
        if not text:
            print("ERROR: redact_content - No text provided")
            return ""
        
        import re
        
        # Remove email addresses
        text = re.sub(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}', '<redacted.email>', text)
        
        # Remove phone numbers (basic pattern)
        text = re.sub(r'\b\d{3}-\d{3}-\d{4}\b', '<redacted.phone>', text)
        
        # Remove IP addresses (but keep example ones)
        text = re.sub(r'\b(?!104\.191\.236\.122)\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '<redacted.ip>', text)
        
        print("DEBUG: redact_content - Content redaction completed")
        return text
        
    except Exception as e:
        print(f"ERROR: redact_content - Error during redaction: {e}")
        return text

def chunk_content(text, size=CHUNK_SIZE):
    """Split content into chunks for manageable file sizes"""
    try:
        print(f"DEBUG: chunk_content - Chunking content of {len(text)} characters")
        
        if not text:
            print("ERROR: chunk_content - No text provided")
            return []
        
        chunks = []
        for i in range(0, len(text), size):
            chunk_index = i // size
            chunk_text = text[i:i+size]
            chunks.append((chunk_index, chunk_text))
        
        print(f"DEBUG: chunk_content - Created {len(chunks)} chunks")
        return chunks
        
    except Exception as e:
        print(f"ERROR: chunk_content - Error chunking content: {e}")
        return []

def save_json_file(path, data):
    """Save data as JSON file with error handling"""
    try:
        print(f"DEBUG: save_json_file - Saving to {path}")
        
        if not ensure_directory(os.path.dirname(path)):
            print(f"ERROR: save_json_file - Failed to create directory for {path}")
            return False
        
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"DEBUG: save_json_file - Successfully saved {path}")
        return True
        
    except Exception as e:
        print(f"ERROR: save_json_file - Error saving {path}: {e}")
        return False

def load_conversations_from_db(limit=1000):
    """Load conversations from database for export"""
    conn = None
    cursor = None
    
    try:
        print(f"DEBUG: load_conversations_from_db - Loading up to {limit} conversations")
        
        conn = get_db_connection()
        if not conn:
            print("ERROR: load_conversations_from_db - No database connection")
            return []
        
        cursor = conn.cursor()
        
        # Get conversations, include public flag if it exists
        query = """
            SELECT id, created_at, source, content, summary
            FROM chats 
            ORDER BY created_at DESC 
            LIMIT %s
        """
        
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()
        
        print(f"DEBUG: load_conversations_from_db - Loaded {len(rows)} conversations")
        return rows
        
    except MySQLError as e:
        print(f"ERROR: load_conversations_from_db - MySQL error: {e}")
        return []
    except Exception as e:
        print(f"ERROR: load_conversations_from_db - Unexpected error: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
            print("DEBUG: load_conversations_from_db - Cursor closed")
        if conn:
            conn.close()
            print("DEBUG: load_conversations_from_db - Connection closed")

def should_export_conversation(content, summary):
    """Determine if conversation should be exported to public mirror"""
    try:
        print("DEBUG: should_export_conversation - Checking export criteria")
        
        if not content:
            print("ERROR: should_export_conversation - No content provided")
            return False, []
        
        # Check for public tags in content and summary
        text_to_check = (content + " " + (summary or "")).lower()
        found_tags = []
        
        for tag in PUBLIC_TAGS:
            if tag in text_to_check:
                found_tags.append(tag)
        
        should_export = len(found_tags) > 0
        
        print(f"DEBUG: should_export_conversation - Export: {should_export}, Tags: {found_tags}")
        return should_export, found_tags
        
    except Exception as e:
        print(f"ERROR: should_export_conversation - Error: {e}")
        return False, []

def export_memory_to_git():
    """Main export function - creates Git mirror from MariaDB"""
    try:
        print(f"DEBUG: export_memory_to_git - Starting export at {datetime.now()}")
        
        # Ensure all directories exist
        for directory in [CATALOG_DIR, SHARD_TAG_DIR, SHARD_DATE_DIR, CHUNK_DIR, META_DIR]:
            if not ensure_directory(directory):
                print(f"ERROR: export_memory_to_git - Failed to create directory: {directory}")
                return False
        
        # Load conversations from database
        conversations = load_conversations_from_db()
        if not conversations:
            print("ERROR: export_memory_to_git - No conversations loaded")
            return False
        
        # Process conversations for export
        items_by_tag = {}
        items_by_month = {}
        exported_count = 0
        
        for chat_id, created_at, source, content, summary in conversations:
            
            # Check if conversation should be exported
            should_export, tags = should_export_conversation(content, summary)
            if not should_export:
                continue
            
            # Redact content for public consumption
            redacted_content = redact_content(content)
            
            # Create chunks for large content
            chunks = chunk_content(redacted_content)
            chunk_urls = []
            
            for chunk_index, chunk_text in chunks:
                chunk_filename = f"{chat_id}-{chunk_index}.json"
                chunk_path = f"{CHUNK_DIR}/{chunk_filename}"
                
                chunk_data = {
                    "id": chat_id,
                    "chunk_ix": chunk_index,
                    "created_at": created_at.isoformat(),
                    "source": source,
                    "tags": sorted(tags),
                    "content": chunk_text
                }
                
                if save_json_file(chunk_path, chunk_data):
                    chunk_urls.append(f"chunks/{chunk_filename}")
            
            # Create item metadata
            item = {
                "id": chat_id,
                "created_at": created_at.isoformat(),
                "source": source,
                "title": (summary or f"{source} conversation")[:80],
                "tags": sorted(tags),
                "summary": summary or "",
                "chunk_urls": chunk_urls,
                "preview_240": redacted_content[:240] + ("..." if len(redacted_content) > 240 else "")
            }
            
            # Group by tags
            for tag in tags:
                if tag not in items_by_tag:
                    items_by_tag[tag] = []
                items_by_tag[tag].append(item)
            
            # Group by month
            month_key = created_at.strftime("%Y-%m")
            if month_key not in items_by_month:
                items_by_month[month_key] = []
            items_by_month[month_key].append(item)
            
            exported_count += 1
        
        # Generate timestamp
        now_iso = datetime.utcnow().isoformat() + "Z"
        
        # Save tag-based shards
        for tag, items in items_by_tag.items():
            shard_data = {
                "version": "1",
                "generated_at": now_iso,
                "items": items
            }
            
            shard_path = f"{SHARD_TAG_DIR}/{tag}.json"
            if not save_json_file(shard_path, shard_data):
                print(f"ERROR: export_memory_to_git - Failed to save tag shard: {tag}")
        
        # Save month-based shards
        for month, items in items_by_month.items():
            shard_data = {
                "version": "1", 
                "generated_at": now_iso,
                "items": items
            }
            
            shard_path = f"{SHARD_DATE_DIR}/{month}.json"
            if not save_json_file(shard_path, shard_data):
                print(f"ERROR: export_memory_to_git - Failed to save month shard: {month}")
        
        # Create search map
        search_tokens = {}
        for tag in items_by_tag.keys():
            search_tokens[tag] = [f"catalog/shards/by_tag/{tag}.json"]
        
        search_map = {
            "version": "1",
            "tokens": search_tokens
        }
        
        if not save_json_file(f"{CATALOG_DIR}/search_map.json", search_map):
            print("ERROR: export_memory_to_git - Failed to save search map")
        
        # Create metadata files
        latest_build = {
            "generated_at": now_iso,
            "exported": exported_count
        }
        
        if not save_json_file(f"{META_DIR}/latest_build.json", latest_build):
            print("ERROR: export_memory_to_git - Failed to save build metadata")
        
        counts = {
            "by_tag": {k: len(v) for k, v in items_by_tag.items()},
            "by_month": {k: len(v) for k, v in items_by_month.items()}
        }
        
        if not save_json_file(f"{META_DIR}/counts.json", counts):
            print("ERROR: export_memory_to_git - Failed to save counts")
        
        # Git operations
        print("DEBUG: export_memory_to_git - Starting git operations")
        
        subprocess.run(["git", "-C", REPO_DIR, "add", "-A"], check=True)
        subprocess.run(["git", "-C", REPO_DIR, "commit", "-m", f"export: refresh public shards - {exported_count} items"], check=False)
        subprocess.run(["git", "-C", REPO_DIR, "push"], check=True)
        
        print(f"DEBUG: export_memory_to_git - Export completed successfully")
        print(f"DEBUG: export_memory_to_git - Exported {exported_count} conversations")
        print(f"DEBUG: export_memory_to_git - Tags: {list(items_by_tag.keys())}")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"ERROR: export_memory_to_git - Git operation failed: {e}")
        return False
    except Exception as e:
        print(f"ERROR: export_memory_to_git - Unexpected error: {e}")
        print(f"ERROR: export_memory_to_git - Traceback: {traceback.format_exc()}")
        return False

if __name__ == '__main__':
    print("=" * 60)
    print("MEMORY SYSTEM GIT MIRROR EXPORTER")
    print(f"Starting export at {datetime.now()}")
    print("=" * 60)
    
    success = export_memory_to_git()
    
    if success:
        print("\nSUCCESS: Git mirror export completed!")
        print("Public conversations exported to Git repository")
        print("AI assistants can now access via raw Git URLs")
    else:
        print("\nFAILED: Git mirror export failed")
        print("Check error messages above for details")
