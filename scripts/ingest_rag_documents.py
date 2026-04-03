"""
RAG document ingestion for Energy Compliance Platform.
Fetches real public CISAF regulatory sources, chunks text,
embeds with sentence-transformers, stores in ChromaDB.
"""

import os
import re
import chromadb
from sentence_transformers import SentenceTransformer
import urllib.request

# ── Config ────────────────────────────────────────────────────────────────────
CHROMA_PATH = os.path.join(os.path.dirname(__file__), "..", "rag", "chromadb")
COLLECTION_NAME = "cisaf_regulatory_docs"
EMBED_MODEL = "all-MiniLM-L6-v2"
CHUNK_SIZE = 400       # words per chunk
CHUNK_OVERLAP = 80     # words overlap between chunks

SOURCES = [
    {
        "id": "gleiss_lutz_cisaf",
        "title": "Germany cuts costs for electricity-intensive companies from 1 January 2026: the new industrial electricity price",
        "source": "Gleiss Lutz (German law firm)",
        "url": "https://www.gleisslutz.com/en/know-how/germany-cuts-costs-electricity-intensive-companies-1-january-2026-new-industrial-electricity-price",
        "date": "2025-12-02",
    },
    {
        "id": "united_gov_affairs_cisaf",
        "title": "Industrial electricity price: What Germany's decision means for EU industrial policy",
        "source": "United Government Affairs",
        "url": "https://www.unitedgovernmentaffairs.com/post/industrial-electricity-price-what-germany-s-decision-means-for-eu-industrial-policy",
        "date": "2025-12-17",
    },
    {
        "id": "pexapark_cisaf",
        "title": "Germany Seals 2026 Industrial Power Price Subsidy",
        "source": "Pexapark (energy market intelligence)",
        "url": "https://pexapark.com/blog/germany-seals-2026-industrial-power-price-subsidy-can-it-help-boost-the-ppa-market/",
        "date": "2025-11-18",
    },
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_url(url):
    """Fetch URL content with a browser-like user agent."""
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def html_to_text(html):
    """Strip HTML tags and normalise whitespace."""
    # Remove script and style blocks
    html = re.sub(r"<(script|style)[^>]*>.*?</(script|style)>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    # Remove all tags
    text = re.sub(r"<[^>]+>", " ", html)
    # Decode common HTML entities
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&nbsp;", " ").replace("&#x27;", "'").replace("&quot;", '"')
    text = text.replace("&#8220;", '"').replace("&#8221;", '"')
    text = text.replace("&#8216;", "'").replace("&#8217;", "'")
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_main_content(text, source_id):
    """
    Extract the most relevant regulatory content from the full page text.
    Each source has identifiable anchor phrases.
    """
    anchors = {
        "gleiss_lutz_cisaf": "Debated for years by politicians",
        "united_gov_affairs_cisaf": "The German government plans to introduce",
        "pexapark_cisaf": "As Germany moves towards introducing",
    }
    end_anchors = {
        "gleiss_lutz_cisaf": "Keep in Touch",
        "united_gov_affairs_cisaf": "Recent Posts",
        "pexapark_cisaf": "Are you interested in unlocking",
    }

    start = text.find(anchors.get(source_id, ""))
    if start == -1:
        print(f"  Warning: anchor not found for {source_id}, using full text")
        return text[:8000]

    end_anchor = end_anchors.get(source_id, "")
    end = text.find(end_anchor, start) if end_anchor else -1
    if end == -1:
        return text[start:start + 8000]
    return text[start:end]


def chunk_text(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    """Split text into overlapping word-based chunks."""
    words = text.split()
    chunks = []
    i = 0
    while i < len(words):
        chunk = " ".join(words[i:i + chunk_size])
        if len(chunk.strip()) > 100:  # skip very short chunks
            chunks.append(chunk)
        i += chunk_size - overlap
    return chunks


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(CHROMA_PATH, exist_ok=True)

    print(f"Loading embedding model: {EMBED_MODEL}")
    model = SentenceTransformer(EMBED_MODEL)
    print("Embedding model loaded.")

    client = chromadb.PersistentClient(path=CHROMA_PATH)

    # Delete and recreate collection for clean re-indexing
    try:
        client.delete_collection(COLLECTION_NAME)
        print(f"Deleted existing collection: {COLLECTION_NAME}")
    except Exception:
        pass

    collection = client.create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"}
    )
    print(f"Created collection: {COLLECTION_NAME}")

    total_chunks = 0

    for source in SOURCES:
        print(f"\nFetching: {source['title']}")
        print(f"  URL: {source['url']}")

        try:
            html = fetch_url(source["url"])
            text = html_to_text(html)
            content = extract_main_content(text, source["id"])
            chunks = chunk_text(content)

            print(f"  Extracted {len(content.split())} words, split into {len(chunks)} chunks")

            embeddings = model.encode(chunks, show_progress_bar=False).tolist()

            ids = [f"{source['id']}_chunk_{i}" for i in range(len(chunks))]
            metadatas = [{
                "source_id": source["id"],
                "title": source["title"],
                "source": source["source"],
                "url": source["url"],
                "date": source["date"],
                "chunk_index": i,
                "total_chunks": len(chunks),
            } for i in range(len(chunks))]

            collection.add(
                ids=ids,
                embeddings=embeddings,
                documents=chunks,
                metadatas=metadatas,
            )

            total_chunks += len(chunks)
            print(f"  Indexed {len(chunks)} chunks from {source['source']}")

        except Exception as e:
            print(f"  ERROR fetching {source['id']}: {e}")
            continue

    print(f"\nIngestion complete. Total chunks indexed: {total_chunks}")
    print(f"ChromaDB stored at: {os.path.abspath(CHROMA_PATH)}")

    # Verify
    count = collection.count()
    print(f"Collection '{COLLECTION_NAME}' contains {count} documents.")


if __name__ == "__main__":
    main()
