import os
import time
import json
import logging
from itertools import islice
import psycopg2
import google.generativeai as genai
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch

SYSTEM_PROMPT = """You are an expert analyst helping a Learning Management System company identify the best conversations to engage with on the internet.

You will be given a conversation title and body. Score it on exactly 5 dimensions as described below. Return ONLY a valid JSON object with no additional text, explanation, or markdown formatting.

Scoring dimensions:
- relevance: How directly relevant is this to an LMS purchasing decision? Score 0-30.
- purchase_intent: How strong are the purchase intent signals? Score 0-25.
- audience_quality: How likely is the poster to be a decision maker or practitioner? Score 0-20.
- response_gap: How much opportunity is there to provide a genuinely useful answer that is not already covered? Score 0-15.
- time_sensitivity: How recent is this conversation? Score 0-10. Use the published_at date if available.

Return exactly this JSON structure:
{
  "relevance": <integer>,
  "purchase_intent": <integer>,
  "audience_quality": <integer>,
  "response_gap": <integer>,
  "time_sensitivity": <integer>,
  "reasoning": "<one sentence explaining the total score>"
}"""

def setup_database_schema(conn):
    try:
        cur = conn.cursor()
        cur.execute("ALTER TABLE conversations ADD COLUMN IF NOT EXISTS score_breakdown JSONB;")
        cur.execute("ALTER TABLE conversations ADD COLUMN IF NOT EXISTS score_reasoning TEXT;")
        conn.commit()
        cur.close()
        logger.info("Database schema updated for scoring columns.")
    except Exception as e:
        logger.error(f"Failed to update database schema: {e}")
        conn.rollback()

def clamp(value, min_val, max_val):
    try:
        val = int(value)
        return max(min_val, min(val, max_val))
    except (ValueError, TypeError):
        return min_val # fallback to 0 if totally invalid

def calculate_priority_label(total_score):
    if total_score >= 85:
        return 'URGENT'
    elif total_score >= 70:
        return 'HIGH'
    elif total_score >= 65:
        return 'MEDIUM'
    else:
        return 'LOW'

def process_batch(batch, model, conn):
    cur = conn.cursor()
    success_count = 0
    fail_count = 0

    for row in batch:
        conv_id = row[0]
        title = row[1]
        body = row[2]
        published_at = row[3]
        
        prompt = f"Title: {title}\nBody: {body}\nPublished At: {published_at}"
        
        try:
            response = model.generate_content(prompt)
            # Clean up the output if gemini decides to wrap in markdown json block
            raw_text = response.text.strip()
            if raw_text.startswith("```json"):
                raw_text = raw_text[7:]
            if raw_text.startswith("```"):
                raw_text = raw_text[3:]
            if raw_text.endswith("```"):
                raw_text = raw_text[:-3]
            raw_text = raw_text.strip()
                
            score_data = json.loads(raw_text)
            
            # Clamp values
            relevance = clamp(score_data.get('relevance', 0), 0, 30)
            purchase_intent = clamp(score_data.get('purchase_intent', 0), 0, 25)
            audience_quality = clamp(score_data.get('audience_quality', 0), 0, 20)
            response_gap = clamp(score_data.get('response_gap', 0), 0, 15)
            time_sensitivity = clamp(score_data.get('time_sensitivity', 0), 0, 10)
            reasoning = str(score_data.get('reasoning', 'No reasoning provided.'))
            
            total_score = relevance + purchase_intent + audience_quality + response_gap + time_sensitivity
            priority_label = calculate_priority_label(total_score)
            
            # Build valid JSONB payload
            final_breakdown = {
                "relevance": relevance,
                "purchase_intent": purchase_intent,
                "audience_quality": audience_quality,
                "response_gap": response_gap,
                "time_sensitivity": time_sensitivity
            }
            
            update_query = """
                UPDATE conversations
                SET
                  priority_score = %(total_score)s,
                  priority_label = %(priority_label)s,
                  status = 'scored',
                  score_breakdown = %(score_breakdown)s,
                  score_reasoning = %(score_reasoning)s
                WHERE id = %(id)s;
            """
            
            cur.execute(update_query, {
                'total_score': total_score,
                'priority_label': priority_label,
                'score_breakdown': json.dumps(final_breakdown),
                'score_reasoning': reasoning,
                'id': conv_id
            })
            success_count += 1
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from Gemini for id {conv_id}: {e}\nRaw={response.text}")
            cur.execute("UPDATE conversations SET status = 'score_failed' WHERE id = %s;", (conv_id,))
            fail_count += 1
        except Exception as e:
            logger.error(f"Error processing id {conv_id}: {e}")
            cur.execute("UPDATE conversations SET status = 'score_failed' WHERE id = %s;", (conv_id,))
            fail_count += 1
            
    conn.commit()
    cur.close()
    return success_count, fail_count

def generate_report(conn):
    try:
        cur = conn.cursor()
        
        # Total scored vs failed
        cur.execute("SELECT status, count(*) FROM conversations GROUP BY status;")
        status_counts = dict(cur.fetchall())
        scored = status_counts.get('scored', 0)
        failed = status_counts.get('score_failed', 0)
        
        # Average priority score
        cur.execute("SELECT avg(priority_score) FROM conversations WHERE status = 'scored';")
        avg_score = cur.fetchone()[0]
        if avg_score is None: avg_score = 0
            
        # Breakdown of labels
        cur.execute("SELECT priority_label, count(*) FROM conversations WHERE status = 'scored' GROUP BY priority_label;")
        labels = dict(cur.fetchall())
        
        logger.info("\n=== END TO END SCORING SUMMARY ===")
        logger.info(f"Rows Scored: {scored}")
        logger.info(f"Rows Failed: {failed}")
        logger.info(f"Average Server Score: {avg_score:.1f}")
        logger.info(f"Labels Breakdown:")
        logger.info(f"  - URGENT: {labels.get('URGENT', 0)}")
        logger.info(f"  - HIGH: {labels.get('HIGH', 0)}")
        logger.info(f"  - MEDIUM: {labels.get('MEDIUM', 0)}")
        logger.info(f"  - LOW: {labels.get('LOW', 0)}")
        
        # Top 5 URGENT
        logger.info("\n=== TOP 5 PRIORITIES ===")
        cur.execute("""
            SELECT title, source, priority_score, priority_label, score_reasoning 
            FROM conversations 
            WHERE status = 'scored' 
            ORDER BY priority_score DESC 
            LIMIT 5;
        """)
        top_5 = cur.fetchall()
        for idx, t in enumerate(top_5, 1):
            logger.info(f"#{idx} [{t[3]} - {t[2]}] (Source: {t[1]})")
            logger.info(f"   Title: {t[0]}")
            logger.info(f"   Reason: {t[4]}")
            
    except Exception as e:
        logger.error(f"Failed to generate evaluation report: {e}")

def main():
    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY environment variable is missing!")
        return
        
    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable is missing!")
        return

    # Setup Gemini
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-flash-latest', system_instruction=SYSTEM_PROMPT)

    logger.info("Connecting to Database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        return

    # Add columns if they don't exist
    setup_database_schema(conn)

    cur = conn.cursor()
    cur.execute("SELECT id, title, body, published_at FROM conversations WHERE status = 'unscored';")
    unscored_rows = cur.fetchall()
    cur.close()

    total_unscored = len(unscored_rows)
    logger.info(f"Found {total_unscored} unscored rows. Beginning processing...")

    total_success = 0
    total_fails = 0
    
    for row in unscored_rows:
        logger.info(f"Processing row id {row[0]}")
        success, fails = process_batch([row], model, conn)
        total_success += success
        total_fails += fails
        
        # Free tier limit is 15 RPM => 1 request every 4 seconds.
        # Spacing out 6-7 seconds to be extra safe.
        time.sleep(7)

    logger.info("Scoring sweep complete!")
    generate_report(conn)
    conn.close()

if __name__ == "__main__":
    main()
