# CHANGE LOG
# File: /srv/memory-git/git_mirror_exporter.py
# Document Type: Python Utility Script
# Purpose: Export memory conversations to Git mirror using nonce-based URLs for security
# Main App: memory_app.py
# Dependencies: mysql.connector, json, os, subprocess, pathlib, secrets
# Context: Creates plain JSON files with hard-to-guess URLs for AI access via GitHub raw
# Security Strategy: URL obscurity with random nonces instead of encryption
# Version History:
# 2025-08-10 v2.0 - Simplified to nonce-based URLs, removed encryption complexity
# 2025-08-10 v1.3 - Fixed function order and directory creation issues
# 2025-08-10 v1.2 - Added incremental updates and change tracking
# 2025-08-10 v1.1 - Added encryption with passphrase-based key
# 2025-08-10 v1.0 - Initial creation following development standards

import os
import json
import subprocess
import traceback
import mysql.connector
from mysql.connector import Error as MySQLError
from datetime import datetime
from pathlib import Path
import secrets

# Configuration
REPO_DIR = "/srv/memory-git"
CATALOG_DIR = f"{REPO_DIR}/catalog"
SHARD_TAG_DIR = f"{CATALOG_DIR}/shards/by_tag"
SHARD_DATE_DIR = f"{CATALOG_DIR}/shards/by_date"
CHUNK_DIR = f"{REPO_DIR}/chunks"
META_DIR = f"{REPO_DIR}/meta"
CHUNK_SIZE = 2000
PUBLIC_TAGS = {"memory", "breakthrough", "historic", "system", "development"}

# Incremental update tracking
LAST_EXPORT_FILE = f"{META_DIR}/last_export.json"
NONCE_MAP_FILE = f"{META_DIR}/nonce_map.json"

def generate_nonce():
    """Generate random nonce for file naming"""
    try:
        nonce = secrets.token_urlsafe(12)  # 12-byte random string
        print(f"DEBUG: generate_nonce - Generated nonce: {nonce}")
        return nonce
    except Exception as e:
        print(f"ERROR: generate_nonce - Error generating nonce: {e}")
        return None

def load_nonce_map():
    """Load existing nonce mappings"""
    try:
        print("DEBUG: load_nonce_map - Loading existing nonce mappings")
        
        if not os.path.exists(NONCE_MAP_FILE):
            print("DEBUG: load_nonce_map - No existing nonce map, creating new one")
            return {}
        
        with open(NONCE_MAP_FILE, 'r', encoding='utf-8') as f:
            nonce_map = json.load(f)
        
        print(f"DEBUG: load_nonce_map - Loaded {len(nonce_map)} nonce mappings")
        return nonce_map
        
    except Exception as e:
        print(f"ERROR: load_nonce_map - Error loading nonce map: {e}")
        return {}

def save_nonce_map(nonce_map):
    """Save nonce mappings for future reference"""
    try:
        print("DEBUG: save_nonce_map - Saving nonce mappings")
        
        with open(NONCE_MAP_FILE, 'w', encoding='utf-8') as f:
            json.dump(nonce_map, f, ensure_ascii=False, indent=2)
        
        print(f"DEBUG: save_nonce_map - Saved {len(nonce_map)} nonce mappings")
        return True
        
    except Exception as e:
        print(f"ERROR: save_nonce_map - Error saving nonce map: {e}")
        return False

def get_nonce_filename(base_name, nonce_map):
    """Get nonce-based filename, creating new nonce if needed"""
    try:
        print(f"DEBUG: get_nonce_filename - Getting nonce filename for {base_name}")
        
        if base_name in nonce_map:
            filename = nonce_map[base_name]
            print(f"DEBUG: get_nonce_filename - Using existing nonce: {filename}")
            return filename
        
        nonce = generate_nonce()
        if not nonce:
            print(f"ERROR: get_nonce_filename - Failed to generate nonce for {base_name}")
            return f"{base_name}.json"  # Fallback
        
        # Create nonce-based filename
        name_parts = base_name.split('.')
        if len(name_parts) > 1:
            filename = f"{name_parts[0]}_{nonce}.{name_parts[1]}"
        else:
            filename = f"{base_name}_{nonce}.json"
        
        nonce_map[base_name] = filename
        print(f"DEBUG: get_nonce_filename - Created new nonce filename: {filename}")
        return filename
        
    except Exception as e:
        print(f"ERROR: get_nonce_filename - Error creating nonce filename: {e}")
        return f"{base_name}.json"  # Fallback

def save_json_file(path, data):
    """Save data as plain JSON file"""
    try:
        print(f"DEBUG: save_json_file - Saving to {path}")
        
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"DEBUG: save_json_file - Successfully saved {path}")
        return True
        
    except Exception as e:
        print(f"ERROR: save_json_file - Error saving {path}: {e}")
        return False

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

def get_last_export_timestamp():
    """Get timestamp of last successful export"""
    try:
        print("DEBUG: get_last_export_timestamp - Checking for previous export")
        
        if not os.path.exists(LAST_EXPORT_FILE):
            print("DEBUG: get_last_export_timestamp - No previous export found, doing full export")
            return None
        
        with open(LAST_EXPORT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            timestamp_str = data.get('last_export_timestamp')
            
            if timestamp_str:
                # Convert ISO string back to datetime
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                print(f"DEBUG: get_last_export_timestamp - Last export: {timestamp}")
                return timestamp
        
        print("DEBUG: get_last_export_timestamp - Could not parse timestamp, doing full export")
        return None
        
    except Exception as e:
        print(f"ERROR: get_last_export_timestamp - Error reading timestamp: {e}")
        return None

def save_export_timestamp(timestamp):
    """Save timestamp of successful export"""
    try:
        print(f"DEBUG: save_export_timestamp - Saving timestamp: {timestamp}")
        
        data = {
            'last_export_timestamp': timestamp.isoformat(),
            'export_completed': True
        }
        
        if save_json_file(LAST_EXPORT_FILE, data):
            print("DEBUG: save_export_timestamp - Timestamp saved successfully")
            return True
        else:
            print("ERROR: save_export_timestamp - Failed to save timestamp")
            return False
            
    except Exception as e:
        print(f"ERROR: save_export_timestamp - Error saving timestamp: {e}")
        return False

def load_conversations_from_db(since_timestamp=None, limit=1000):
    """Load conversations from database for export - incremental if timestamp provided"""
    conn = None
    cursor = None
    
    try:
        if since_timestamp:
            print(f"DEBUG: load_conversations_from_db - Loading conversations since {since_timestamp}")
        else:
            print(f"DEBUG: load_conversations_from_db - Loading all conversations (full export)")
        
        conn = get_db_connection()
        if not conn:
            print("ERROR: load_conversations_from_db - No database connection")
            return []
        
        cursor = conn.cursor()
        
        # Build query based on incremental vs full export
        if since_timestamp:
            query = """
                SELECT id, created_at, source, content, summary
                FROM chats 
                WHERE created_at > %s
                ORDER BY created_at DESC 
                LIMIT %s
            """
            cursor.execute(query, (since_timestamp, limit))
        else:
            query = """
                SELECT id, created_at, source, content, summary
                FROM chats 
                ORDER BY created_at DESC 
                LIMIT %s
            """
            cursor.execute(query, (limit,))
        
        rows = cursor.fetchall()
        
        if since_timestamp:
            print(f"DEBUG: load_conversations_from_db - Loaded {len(rows)} new conversations since last export")
        else:
            print(f"DEBUG: load_conversations_from_db - Loaded {len(rows)} total conversations")
        
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

def export_memory_to_git():
    """Main export function - creates nonce-based Git mirror from MariaDB"""
    try:
        print(f"DEBUG: export_memory_to_git - Starting nonce-based export at {datetime.now()}")
        
        # Load existing nonce mappings
        nonce_map = load_nonce_map()
        
        # Check for incremental vs full export
        last_export = get_last_export_timestamp()
        is_incremental = last_export is not None
        
        if is_incremental:
            print(f"DEBUG: export_memory_to_git - Incremental export since {last_export}")
        else:
            print("DEBUG: export_memory_to_git - Full export (first run)")
        
        # Load conversations from database (incremental if possible)
        conversations = load_conversations_from_db(since_timestamp=last_export)
        
        if not conversations:
            if is_incremental:
                print("DEBUG: export_memory_to_git - No new conversations since last export")
                return True  # Success - just nothing new
            else:
                print("ERROR: export_memory_to_git - No conversations loaded")
                return False
        
        # Process new conversations
        new_items_by_tag = {}
        new_items_by_month = {}
        new_exported_count = 0
        
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
                # Create nonce-based chunk filename
                chunk_base_name = f"{chat_id}-{chunk_index}"
                chunk_filename = get_nonce_filename(chunk_base_name, nonce_map)
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
                if tag not in new_items_by_tag:
                    new_items_by_tag[tag] = []
                new_items_by_tag[tag].append(item)
            
            # Group by month
            month_key = created_at.strftime("%Y-%m")
            if month_key not in new_items_by_month:
                new_items_by_month[month_key] = []
            new_items_by_month[month_key].append(item)
            
            new_exported_count += 1
        
        # For simplicity, just use new items (no merging for now)
        final_items_by_tag = new_items_by_tag
        final_items_by_month = new_items_by_month
        
        # Generate timestamp
        now_iso = datetime.utcnow().isoformat() + "Z"
        
        # Save tag-based shards with nonce filenames
        for tag, items in final_items_by_tag.items():
            shard_data = {
                "version": "1",
                "generated_at": now_iso,
                "items": items
            }
            
            shard_filename = get_nonce_filename(f"{tag}", nonce_map)
            shard_path = f"{SHARD_TAG_DIR}/{shard_filename}"
            if not save_json_file(shard_path, shard_data):
                print(f"ERROR: export_memory_to_git - Failed to save tag shard: {tag}")
        
        # Save month-based shards with nonce filenames
        for month, items in final_items_by_month.items():
            shard_data = {
                "version": "1", 
                "generated_at": now_iso,
                "items": items
            }
            
            month_filename = get_nonce_filename(f"{month}", nonce_map)
            shard_path = f"{SHARD_DATE_DIR}/{month_filename}"
            if not save_json_file(shard_path, shard_data):
                print(f"ERROR: export_memory_to_git - Failed to save month shard: {month}")
        
        # Create search map with nonce-based URLs
        search_tokens = {}
        for tag in final_items_by_tag.keys():
            tag_filename = nonce_map.get(tag, f"{tag}.json")
            search_tokens[tag] = [f"catalog/shards/by_tag/{tag_filename}"]
        
        search_map = {
            "version": "1",
            "tokens": search_tokens,
            "security": "nonce_based_urls",
            "access_note": "URLs use random nonces for security through obscurity"
        }
        
        # Search map gets its own nonce
        search_map_filename = get_nonce_filename("search_map", nonce_map)
        if not save_json_file(f"{CATALOG_DIR}/{search_map_filename}", search_map):
            print("ERROR: export_memory_to_git - Failed to save search map")
        
        # Save nonce mappings
        if not save_nonce_map(nonce_map):
            print("ERROR: export_memory_to_git - Failed to save nonce mappings")
        
        # Create metadata files
        total_exported = sum(len(items) for items in final_items_by_tag.values())
        latest_build = {
            "generated_at": now_iso,
            "total_conversations": total_exported,
            "new_conversations": new_exported_count,
            "incremental_update": is_incremental,
            "security": "nonce_based_urls",
            "search_map_url": f"catalog/{search_map_filename}"
        }
        
        build_filename = get_nonce_filename("latest_build", nonce_map)
        if not save_json_file(f"{META_DIR}/{build_filename}", latest_build):
            print("ERROR: export_memory_to_git - Failed to save build metadata")
        
        # Git operations
        print("DEBUG: export_memory_to_git - Starting git operations")
        
        subprocess.run(["git", "-C", REPO_DIR, "add", "-A"], check=True)
        
        if is_incremental:
            commit_message = f"incremental: {new_exported_count} new conversations (nonce-based)"
        else:
            commit_message = f"full export: {total_exported} total conversations (nonce-based)"
            
        subprocess.run(["git", "-C", REPO_DIR, "commit", "-m", commit_message], check=False)
        subprocess.run(["git", "-C", REPO_DIR, "push"], check=True)
        
        # Save timestamp of successful export
        export_timestamp = datetime.now()
        if not save_export_timestamp(export_timestamp):
            print("ERROR: export_memory_to_git - Failed to save export timestamp")
            return False
        
        print(f"DEBUG: export_memory_to_git - Export completed successfully")
        
        if is_incremental:
            print(f"DEBUG: export_memory_to_git - Incremental update: {new_exported_count} new conversations")
            print(f"DEBUG: export_memory_to_git - Total conversations now: {total_exported}")
        else:
            print(f"DEBUG: export_memory_to_git - Full export: {total_exported} conversations")
        
        print(f"DEBUG: export_memory_to_git - Tags: {list(final_items_by_tag.keys())}")
        print(f"DEBUG: export_memory_to_git - Search map URL: catalog/{search_map_filename}")
        
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
    print("SIMPLE NONCE-BASED MEMORY SYSTEM GIT MIRROR EXPORTER v2.0")
    print(f"Starting export at {datetime.now()}")
    print("Security: Nonce-based URLs for AI accessibility")
    print("=" * 60)
    
    success = export_memory_to_git()
    
    if success:
        print("\nSUCCESS: Simple nonce-based Git mirror export completed!")
        print("All conversations saved as plain JSON with nonce-based URLs")
        print("AI assistants can directly access content with the nonce URLs")
        print("Check meta/nonce_map.json for URL mappings")
    else:
        print("\nFAILED: Simple nonce-based Git mirror export failed")
        print("Check error messages above for details")
