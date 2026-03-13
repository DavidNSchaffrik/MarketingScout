import os
import psycopg
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()

EMBED_MODEL = "BAAI/bge-small-en-v1.5"
BATCH_SIZE = 256

model = SentenceTransformer(EMBED_MODEL)

def get_db_connection():
    return psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )

def fetch_unembed_batch(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                ic.item_content_id,
                concat_ws(E'\n',
                    'Subforum: ' || COALESCE(c.subforum, 'Unknown'),
                    'Thread: ' || COALESCE(c.thread_title, 'Unknown'),
                    'Author: ' || a.handle
                        || ' | Reactions: ' || COALESCE(a.reaction_score::text, '0')
                        || ' | Posts: ' || COALESCE(a.post_count::text, '0'),
                    'Position: ' || i.position_in_thread,
                    '',
                    ic.content_text
                ) AS chunk_text
            FROM item_content ic
            JOIN item i ON i.item_id = ic.item_id
            JOIN container c ON c.container_id = i.container_id
            JOIN actor a ON a.actor_id = i.actor_id
            WHERE ic.is_current = true
              AND NOT EXISTS (
                SELECT 1 FROM item_embedding ie
                WHERE ie.item_content_id = ic.item_content_id
                  AND ie.model = %s
              )
            LIMIT %s
        """, (EMBED_MODEL, BATCH_SIZE))
        return cur.fetchall()

def save_embeddings(conn, rows, embeddings):
    with conn.cursor() as cur:
        for (item_content_id, chunk_text), embedding in zip(rows, embeddings):
            cur.execute("""
                INSERT INTO item_embedding (
                    item_content_id,
                    model,
                    chunk_text,
                    embedding
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (item_content_id, model, chunk_index) DO NOTHING
            """, (
                item_content_id,
                EMBED_MODEL,
                chunk_text,
                embedding.tolist(),
            ))
    conn.commit()

def run_embedder():
    conn = get_db_connection()
    total = 0

    print("Starting embedding worker...")
    print("First run will download the model (~90MB), this is normal.")

    try:
        while True:
            rows = fetch_unembed_batch(conn)
            if not rows:
                print(f"Done. Total embedded: {total}")
                break

            texts = [row[1] for row in rows]
            embeddings = model.encode(texts, show_progress_bar=True)
            save_embeddings(conn, rows, embeddings)

            total += len(rows)
            print(f"Embedded {total} posts so far...")

    finally:
        conn.close()

if __name__ == "__main__":
    run_embedder()