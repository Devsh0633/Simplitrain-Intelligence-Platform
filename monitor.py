import os
import time
import logging
from datetime import datetime, timezone
from dateutil import parser
import json
import uuid

import psycopg2
import psycopg2.extras
import feedparser
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
SERPER_API_KEY = os.environ.get("SERPER_API_KEY")

# Tiers
TIER_1_KEYWORDS = [
    "lms", "learning management system", "scorm", "xapi", "ppt to scorm", 
    "powerpoint to scorm", "simplitrain", "lms recommendation", "best lms", "which lms"
]

TIER_2_KEYWORDS = [
    "elearning platform", "e-learning platform", "corporate training software", 
    "employee training platform", "employee training software", "lms comparison", 
    "lms review", "lms alternative", "lms pricing", "lms implementation", 
    "training platform", "compliance training", "onboarding platform", "lms vs", 
    "docebo", "talentlms", "moodle", "cornerstone ondemand", "360learning", 
    "absorb lms", "litmos", "ispring"
]

def check_keywords(text):
    text = text.lower()
    for kw in TIER_1_KEYWORDS:
        if kw in text:
            return kw, 1, 'Tier 1'
    for kw in TIER_2_KEYWORDS:
        if kw in text:
            return kw, 2, 'Tier 2'
    return None, None, None

def setup_database():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Create table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
              id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
              source TEXT NOT NULL,
              source_detail TEXT,
              title TEXT NOT NULL,
              body TEXT,
              url TEXT UNIQUE NOT NULL,
              author TEXT,
              published_at TIMESTAMPTZ,
              retrieved_at TIMESTAMPTZ DEFAULT NOW(),
              keyword_matched TEXT,
              priority_score INTEGER,
              priority_label TEXT,
              status TEXT DEFAULT 'unscored',
              created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        
        # Create indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_status ON conversations(status);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_source ON conversations(source);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_priority ON conversations(priority_score DESC);")
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database setup completed successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to setup database: {str(e)}")
        return False

# --- Fetchers ---

def fetch_reddit():
    logger.info("Fetching from Reddit RSS...")
    urls = [
        "https://www.reddit.com/r/elearning/.rss",
        "https://www.reddit.com/r/instructionaldesign/.rss",
        "https://www.reddit.com/r/humanresources/.rss",
        "https://www.reddit.com/r/lms/.rss",
        "https://www.reddit.com/r/training/.rss",
        "https://www.reddit.com/r/elearning/search.rss?q=LMS+recommendation&sort=new&restrict_sr=1",
        "https://www.reddit.com/r/elearning/search.rss?q=SCORM&sort=new&restrict_sr=1",
        "https://www.reddit.com/r/humanresources/search.rss?q=learning+management+system&sort=new&restrict_sr=1"
    ]
    results = []
    headers = {'User-Agent': 'SimpliTrain-Monitor/1.0'}
    
    for url in urls:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            feed = feedparser.parse(response.content)
            
            # Extract subreddit from URL heuristically
            source_detail = ""
            if '/r/' in url:
                parts = url.split('/r/')
                if len(parts) > 1:
                    source_detail = "r/" + parts[1].split('/')[0]

            for entry in feed.entries:
                published_at = None
                if hasattr(entry, 'published'):
                    try:
                        published_at = parser.parse(entry.published)
                    except:
                        pass
                
                body = entry.summary if hasattr(entry, 'summary') else ""
                # remove html tags from reddit body
                soup = BeautifulSoup(body, 'html.parser')
                clean_body = soup.get_text()

                results.append({
                    "title": entry.title,
                    "body": clean_body,
                    "url": entry.link,
                    "author": entry.author if hasattr(entry, 'author') else "",
                    "published_at": published_at,
                    "source": "reddit",
                    "source_detail": source_detail
                })
        except Exception as e:
            logger.error(f"Error fetching Reddit feed {url}: {str(e)}")
            
    return results

def fetch_serper():
    logger.info("Fetching from Serper.dev API...")
    queries = [
        "best LMS 2025",
        "LMS recommendation",
        "LMS comparison site:reddit.com OR site:quora.com",
        "SCORM help",
        "PPT to SCORM converter",
        "learning management system review",
        "LMS alternative",
        "corporate LMS problems",
        "simplitrain"
    ]
    
    if not SERPER_API_KEY:
        logger.warning("SERPER_API_KEY is not set. Skipping Serper source.")
        return []

    url = "https://google.serper.dev/search"
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }
    
    results = []
    for q in queries:
        try:
            payload = json.dumps({
                "q": q,
                "tbs": "qdr:d"
            })
            response = requests.post(url, headers=headers, data=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get("organic", []):
                link = item.get("link", "")
                domain = link.split('/')[2] if link else ""
                
                results.append({
                    "title": item.get("title", ""),
                    "body": item.get("snippet", ""),
                    "url": link,
                    "author": "",
                    "published_at": None,
                    "source": "web",
                    "source_detail": domain
                })
            time.sleep(2)  # Avoid rate limit
        except Exception as e:
            logger.error(f"Error fetching Serper query '{q}': {str(e)}")
            
    return results

def fetch_quora():
    logger.info("Fetching from Quora RSS...")
    urls = [
        "https://www.quora.com/topic/Learning-Management-Systems/feed",
        "https://www.quora.com/topic/E-Learning/feed",
        "https://www.quora.com/topic/Corporate-Training/feed",
        "https://www.quora.com/topic/SCORM/feed"
    ]
    results = []
    
    for url in urls:
        try:
            feed = feedparser.parse(url)
            topic = url.split('/')[-2] if len(url.split('/')) > 2 else "unknown"
            
            for entry in feed.entries:
                published_at = None
                if hasattr(entry, 'published'):
                    try:
                        published_at = parser.parse(entry.published)
                    except:
                        pass
                
                body = entry.summary if hasattr(entry, 'summary') else ""
                soup = BeautifulSoup(body, 'html.parser')
                clean_body = soup.get_text()
                
                results.append({
                    "title": entry.title,
                    "body": clean_body,
                    "url": entry.link,
                    "author": entry.author if hasattr(entry, 'author') else "",
                    "published_at": published_at,
                    "source": "quora",
                    "source_detail": topic
                })
        except Exception as e:
            logger.error(f"Error fetching Quora feed {url}: {str(e)}")
            
    return results

def fetch_forums():
    logger.info("Fetching from Industry Forums RSS...")
    urls = [
        "https://elearningindustry.com/feed",
        "https://trainingindustry.com/feed/"
    ]
    results = []
    
    for url in urls:
        try:
            feed = feedparser.parse(url)
            domain = dict([("elearningindustry.com", "elearningindustry.com"), ("trainingindustry.com", "trainingindustry.com")]).get(url.split('/')[2], "forum")
            
            for entry in feed.entries:
                published_at = None
                if hasattr(entry, 'published'):
                    try:
                        published_at = parser.parse(entry.published)
                    except:
                        pass
                
                body = entry.summary if hasattr(entry, 'summary') else ""
                soup = BeautifulSoup(body, 'html.parser')
                clean_body = soup.get_text()
                
                results.append({
                    "title": entry.title,
                    "body": clean_body,
                    "url": entry.link,
                    "author": entry.author if hasattr(entry, 'author') else "",
                    "published_at": published_at,
                    "source": "forum",
                    "source_detail": url.split('/')[2]
                })
        except Exception as e:
            logger.error(f"Error fetching Forum feed {url}: {str(e)}")
            
    return results

def fetch_stackoverflow():
    logger.info("Fetching from Stack Overflow API...")
    urls = [
        "https://api.stackexchange.com/2.3/questions?tagged=scorm&sort=creation&order=desc&site=stackoverflow&pagesize=20&filter=withbody",
        "https://api.stackexchange.com/2.3/questions?tagged=xapi&sort=creation&order=desc&site=stackoverflow&pagesize=20&filter=withbody"
    ]
    results = []
    
    for url in urls:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get("items", []):
                published_at = datetime.fromtimestamp(item.get("creation_date", 0), tz=timezone.utc)
                body_html = item.get("body", "")
                soup = BeautifulSoup(body_html, 'html.parser')
                clean_body = soup.get_text()
                
                author = ""
                owner = item.get("owner", {})
                if isinstance(owner, dict):
                    author = owner.get("display_name", "")

                results.append({
                    "title": item.get("title", ""),
                    "body": clean_body,
                    "url": f"https://stackoverflow.com/questions/{item.get('question_id')}",
                    "author": author,
                    "published_at": published_at,
                    "source": "stackoverflow",
                    "source_detail": "Stack Overflow"
                })
        except Exception as e:
            logger.error(f"Error fetching Stack Overflow {url}: {str(e)}")
            
    return results

def run_fetcher_with_retry(fetcher_func):
    try:
        return fetcher_func()
    except Exception as e:
        logger.error(f"Fetcher {fetcher_func.__name__} failed: {e}. Retrying in 5 seconds...")
        time.sleep(5)
        try:
            return fetcher_func()
        except Exception as retry_e:
            logger.error(f"Fetcher {fetcher_func.__name__} failed on retry: {retry_e}.")
            return []

def main():
    start_time = time.time()
    logger.info("Starting SimpliTrain Monitor Pipeline...")
    
    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable is not set. Exiting.")
        return

    if not setup_database():
        logger.error("Database setup failed. Exiting.")
        return

    # 1. Fetch data from all sources
    all_results = []
    for fetcher in [fetch_reddit, fetch_serper, fetch_quora, fetch_forums, fetch_stackoverflow]:
        all_results.extend(run_fetcher_with_retry(fetcher))
        
    logger.info(f"Total raw items fetched: {len(all_results)}")
    
    # Connect to DB for inserted data
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
    except Exception as e:
        logger.error(f"Failed to connect to database for insertion: {e}")
        return

    new_inserted = 0
    discarded = 0
    
    for item in all_results:
        title = item.get('title', '')
        body = item.get('body', '')
        text_to_check = f"{title} {body}"
        
        # a. Keyword filter
        keyword, score, label = check_keywords(text_to_check)
        if not keyword:
            discarded += 1
            continue
            
        # b. Truncate body
        if len(body) > 2000:
            body = body[:1997] + "..."
            
        # c & d. Insert using ON CONFLICT DO NOTHING
        insert_query = """
            INSERT INTO conversations (
                source, source_detail, title, body, url, author, 
                published_at, keyword_matched, priority_score, priority_label
            ) VALUES (
                %(source)s, %(source_detail)s, %(title)s, %(body)s, %(url)s, %(author)s,
                %(published_at)s, %(keyword_matched)s, %(priority_score)s, %(priority_label)s
            ) ON CONFLICT (url) DO NOTHING;
        """
        
        try:
            cur.execute(insert_query, {
                'source': item.get('source'),
                'source_detail': item.get('source_detail'),
                'title': title,
                'body': body,
                'url': item.get('url'),
                'author': item.get('author'),
                'published_at': item.get('published_at'),
                'keyword_matched': keyword,
                'priority_score': score,
                'priority_label': label
            })
            if cur.rowcount > 0:
                new_inserted += 1
        except Exception as e:
            logger.error(f"Error inserting url {item.get('url')}: {e}")
            conn.rollback() # Important to rollback on error to keep the connection usable
        else:
            conn.commit()

    cur.close()
    conn.close()
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    logger.info("--- Pipeline Summary ---")
    logger.info(f"Total Run Time: {elapsed:.2f} seconds")
    logger.info(f"Raw Items Processed: {len(all_results)}")
    logger.info(f"Items Discarded (no keyword): {discarded}")
    logger.info(f"New Conversations Stored: {new_inserted}")
    
if __name__ == "__main__":
    main()
