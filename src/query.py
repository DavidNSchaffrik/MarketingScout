import os
import psycopg
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()

EMBED_MODEL = "BAAI/bge-small-en-v1.5"
QUERY_PREFIX = "Represent this sentence for searching relevant passages: "

model = SentenceTransformer(EMBED_MODEL)

def get_db_connection():
    return psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )

def retrieve(question: str, limit: int = 5) -> list[dict]:
    question_embedding = model.encode(QUERY_PREFIX + question).tolist()

    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                ie.chunk_text,
                c.thread_title,
                c.subforum,
                c.canonical_url,
                a.handle,
                a.reaction_score,
                i.score AS like_count,
                ie.embedding <=> %s::vector AS distance
            FROM item_embedding ie
            JOIN item_content ic ON ic.item_content_id = ie.item_content_id
            JOIN item i ON i.item_id = ic.item_id
            JOIN container c ON c.container_id = i.container_id
            JOIN actor a ON a.actor_id = i.actor_id
            WHERE LENGTH(ic.content_text) > 200
            ORDER BY distance ASC
            LIMIT %s
        """, (question_embedding, limit))

        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    conn.close()
    return rows

if __name__ == "__main__":
    question = input("Ask a question: ")
    results = retrieve(question)

    for i, r in enumerate(results, 1):
        print(f"\n--- Result {i} ---")
        print(f"Thread: {r['thread_title']}")
        print(f"Subforum: {r['subforum']}")
        print(f"Author: {r['handle']} | Reactions: {r['reaction_score']}")
        print(f"Distance: {r['distance']:.4f}")
        print(f"URL: {r['canonical_url']}")
        print(f"\n{r['chunk_text'][:300]}...")