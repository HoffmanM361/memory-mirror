# CHANGE LOG
# File: /srv/memory-git/git_mirror_exporter.py
# Document Type: Python Utility Script
# Purpose: Export memory conversations with simple token-based URLs for universal AI access
# Main App: memory_app.py
# Dependencies: mysql.connector, json, os, subprocess, pathlib, secrets
# Context: Creates plain JSON files with simple URLs + token parameters for AI access
# Security Strategy: Simple token parameters (?key=TOKEN) for basic access control
# Version History:
# 2025-08-10 v3.0 - Simple token-based URLs with automatic URL dictionary generation
# 2025-08-10 v2.0 - Simplified to nonce-based URLs, removed encryption complexity
# 2025-08-10 v1.3 - Fixed function order and directory creation issues

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

# Token configuration
TOKEN_FILE = f"{META_DIR}/access_token.txt"
URL_DICTIONARY_FILE = f"{META_DIR}/url_dictionary.json"
GITHUB_BASE_URL = "https://raw.githubusercontent.com/HoffmanM361/memory-mirror/main"

def generate_or_load_token():
    """Generate new token or load existing one"""
    try:
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, 'r', encoding='utf-8') as f:
                token = f.read().strip()
            print(f"DEBUG: generate_or_load_token - Using existing token: {token}")
            return token
        
        # Generate new token
        token = secrets.token_hex(8)  # 16-character hex string
        
        with open(TOKEN_FILE, 'w', encoding='utf-8') as f:
            f.write(token)
        
        print(f"DEBUG: generate_or_load_token - Generated new token: {token}")
        return token
        
    except Exception as e:
        print(f"ERROR: generate_or_load_token - Error with token: {e}")
        return "fallback_token_12345678"

def build_url_dictionary(token):
    """Build dictionary of all URLs with tokens for easy access"""
    try:
        print("DEBUG: build_url_dictionary - Creating URL dictionary")
        
        url_dict = {
            "access_info": {
                "token": token,
                "instructions": "Append '?key=' + token to any URL for access",
                "example": f"{GITHUB_BASE_URL}/catalog/search_map.json?key={token}"
            },
            "main_entry": {
                "search_map": f"{GITHUB_BASE_URL}/catalog/search_map.json?key={token}",
                "description": "Start here - contains navigation to all topics"
            },
            "topic_shards": {
                "memory": f"{GITHUB_BASE_URL}/catalog/shards/by_tag/memory.json?key={token}",
                "breakthrough": f"{GITHUB_BASE_URL}/catalog/shards/by_tag/breakthrough.json?key={token}",
                "historic": f"{GITHUB_BASE_URL}/catalog/shards/by_tag/historic.json?key={token}",
                "system": f"{GITHUB_BASE_URL}/catalog/shards/by_tag/system.json?key={token}",
                "development": f"{GITHUB_BASE_URL}/catalog/shards/by_tag/development.json?key={token}"
            },
            "metadata": {
                "latest_build": f"{GITHUB_BASE_URL}/meta/latest_build.json?key={token}",
                "counts": f"{GITHUB_BASE_URL}/meta/counts.json?key={token}",
                "url_dictionary": f"{GITHUB_BASE_URL}/meta/url_dictionary.json?key={token}"
            },
            "date_shards": {
                "2025-08": f"{GITHUB_BASE_URL}/catalog/shards/by_date/2025-08.json?key={token}"
            },
            "note": "Individual conversation chunk URLs are listed in each shard's 'chunk_urls' field"
        }
        
        print(f"DEBUG: build_url_dictionary - Created dictionary with {len(url_dict)} sections")
        return url_dict
        
    except Exception as e:
        print(f"ERROR: build_url_dictionary - Error creating URL dictionary: {e}")
        return {}

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
    """Main export function - creates simple token-based Git mirror from MariaDB"""
    try:
        print(f"DEBUG: export_memory_to_git - Starting token-based export at {datetime.now()}")
        
        # Generate or load access token
        token = generate_or_load_token()
        
        # Load conversations from database
        conversations = load_conversations_from_db()
        
        if not conversations:
            print("ERROR: export_memory_to_git - No conversations loaded")
            return False
        
        # Process conversations
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
                # Simple chunk filename
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
                    # URLs include token parameter
                    chunk_urls.append(f"chunks/{chunk_filename}?key={token}")
            
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
        
        # Save tag-based shards with simple names
        for tag, items in items_by_tag.items():
            shard_data = {
                "version": "1",
                "generated_at": now_iso,
                "items": items
            }
            
            shard_path = f"{SHARD_TAG_DIR}/{tag}.json"
            if not save_json_file(shard_path, shard_data):
                print(f"ERROR: export_memory_to_git - Failed to save tag shard: {tag}")
        
        # Save month-based shards with simple names
        for month, items in items_by_month.items():
            shard_data = {
                "version": "1", 
                "generated_at": now_iso,
                "items": items
            }
            
            shard_path = f"{SHARD_DATE_DIR}/{month}.json"
            if not save_json_file(shard_path, shard_data):
                print(f"ERROR: export_memory_to_git - Failed to save month shard: {month}")
        
        # Create search map with simple URLs
        search_tokens = {}
        for tag in items_by_tag.keys():
            search_tokens[tag] = [f"catalog/shards/by_tag/{tag}.json?key={token}"]
        
        search_map = {
            "version": "1",
            "tokens": search_tokens,
            "security": "token_parameter",
            "access_token": token,
            "access_note": "Append ?key=" + token + " to any URL for access"
        }
        
        # Save search map with simple name
        if not save_json_file(f"{CATALOG_DIR}/search_map.json", search_map):
            print("ERROR: export_memory_to_git - Failed to save search map")
        
        # Create and save URL dictionary
        url_dictionary = build_url_dictionary(token)
        if not save_json_file(URL_DICTIONARY_FILE, url_dictionary):
            print("ERROR: export_memory_to_git - Failed to save URL dictionary")
        
        # Create metadata files
        latest_build = {
            "generated_at": now_iso,
            "total_conversations": exported_count,
            "security": "token_parameter",
            "access_token": token,
            "url_dictionary_location": "meta/url_dictionary.json"
        }
        
        if not save_json_file(f"{META_DIR}/latest_build.json", latest_build):
            print("ERROR: export_memory_to_git - Failed to save build metadata")
        
        counts = {
            "by_tag": {k: len(v) for k, v in items_by_tag.items()},
            "by_month": {k: len(v) for k, v in items_by_month.items()},
            "access_token": token
        }
        
        if not save_json_file(f"{META_DIR}/counts.json", counts):
            print("ERROR: export_memory_to_git - Failed to save counts")
        
        # Git operations
        print("DEBUG: export_memory_to_git - Starting git operations")
        
        subprocess.run(["git", "-C", REPO_DIR, "add", "-A"], check=True)
        subprocess.run(["git", "-C", REPO_DIR, "commit", "-m", f"simple token export: {exported_count} conversations"], check=False)
        subprocess.run(["git", "-C", REPO_DIR, "push"], check=True)
        
        print(f"DEBUG: export_memory_to_git - Export completed successfully")
        print(f"DEBUG: export_memory_to_git - Exported {exported_count} conversations")
        print(f"DEBUG: export_memory_to_git - Access token: {token}")
        print(f"DEBUG: export_memory_to_git - Tags: {list(items_by_tag.keys())}")
        
        # Print URL dictionary for easy access
        print("\n" + "="*60)
        print("🔑 ACCESS TOKEN:", token)
        print("📋 READY-TO-USE URLs:")
        print("="*60)
        print(f"🗺️  Search Map: {GITHUB_BASE_URL}/catalog/search_map.json?key={token}")
        print(f"📊 URL Dictionary: {GITHUB_BASE_URL}/meta/url_dictionary.json?key={token}")
        print("\n📚 Topic Shards:")
        for tag in items_by_tag.keys():
            print(f"   {tag.title()}: {GITHUB_BASE_URL}/catalog/shards/by_tag/{tag}.json?key={token}")
        print("="*60)
        
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
    print("SIMPLE TOKEN-BASED MEMORY SYSTEM GIT MIRROR EXPORTER v3.0")
    print(f"Starting export at {datetime.now()}")
    print("Security: Simple token parameters for universal AI access")
    print("=" * 60)
    
    success = export_memory_to_git()
    
    if success:
        print("\nSUCCESS: Simple token-based Git mirror export completed!")
        print("All conversations saved with simple URLs + token parameters")
        print("AI assistants (Claude, ChatGPT, etc.) can access with token URLs")
        print("Check the URLs printed above for immediate access!")
    else:
        print("\nFAILED: Simple token-based Git mirror export failed")
        print("Check error messages above for details")
