"""
RAG query interface for Energy Compliance Platform.
Retrieves relevant CISAF regulatory chunks from ChromaDB,
combines with live Databricks compliance data where relevant,
generates grounded answers via Groq API, and runs an NLI-based
hallucination gate to flag low-faithfulness answers before serving.
"""

import os
import chromadb
from sentence_transformers import SentenceTransformer, CrossEncoder
from groq import Groq
from databricks import sql

# ── Config ────────────────────────────────────────────────────────────────────
CHROMA_PATH = os.path.join(os.path.dirname(__file__), "..", "rag", "chromadb")
COLLECTION_NAME = "cisaf_regulatory_docs"
EMBED_MODEL = "all-MiniLM-L6-v2"
NLI_MODEL = "cross-encoder/nli-deberta-v3-small"
FAITHFULNESS_THRESHOLD = 2.0  # entailment score below this triggers flag
TOP_K = 3

DATABRICKS_HOST = "dbc-174e65df-4c24.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/d7ae726d9291a51f"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")


# ── Hallucination gate ────────────────────────────────────────────────────────

def check_faithfulness(answer, chunks, nli_model):
    """
    Score the answer against each retrieved chunk using a cross-encoder NLI model.
    Returns the max entailment score across all chunks and a flag.
    
    The cross-encoder takes (premise, hypothesis) pairs and returns scores for
    [contradiction, neutral, entailment]. We take the entailment score.
    If the best entailment score across all chunks is below the threshold,
    the answer is flagged as potentially hallucinated.
    """
    chunk_texts = [c["text"] for c in chunks]
    pairs = [(chunk, answer) for chunk in chunk_texts]
    
    scores = nli_model.predict(pairs)
    # scores shape: (n_chunks, 3) -- [contradiction, neutral, entailment]
    entailment_scores = [s[2] for s in scores]
    max_score = max(entailment_scores)
    best_chunk_idx = entailment_scores.index(max_score)
    
    flagged = max_score < FAITHFULNESS_THRESHOLD
    return {
        "max_entailment_score": round(max_score, 4),
        "best_supporting_chunk": chunks[best_chunk_idx]["source"],
        "flagged": flagged,
        "all_scores": [round(s, 4) for s in entailment_scores],
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_compliance_data():
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    facility_name,
                    sector,
                    compliance_status,
                    total_subsidy_eur,
                    confirmed_investment_eur,
                    reinvestment_obligation_eur,
                    compliance_pct,
                    flexibility_bonus_eligible
                FROM workspace.energy_compliance.facility_compliance_summary
                ORDER BY compliance_pct ASC
            """)
            return cursor.fetchall()


def format_compliance_data(rows):
    lines = ["Live facility compliance data (as of latest pipeline run):"]
    for r in rows:
        lines.append(
            f"  - {r[0]} ({r[1]}): status={r[2]}, "
            f"subsidy=EUR {float(r[3]):,.2f}, "
            f"invested=EUR {float(r[4]):,.2f}, "
            f"obligation=EUR {float(r[5]):,.2f}, "
            f"compliance={float(r[6]):.1f}%, "
            f"flexibility_bonus_eligible={r[7]}"
        )
    return "\n".join(lines)


def retrieve_chunks(question, model, collection, top_k=TOP_K):
    embedding = model.encode([question])[0].tolist()
    results = collection.query(
        query_embeddings=[embedding],
        n_results=top_k,
        include=["documents", "metadatas", "distances"],
    )
    chunks = []
    for doc, meta, dist in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0],
    ):
        chunks.append({
            "text": doc,
            "source": meta["source"],
            "title": meta["title"],
            "url": meta["url"],
            "date": meta["date"],
            "similarity": round(1 - dist, 3),
        })
    return chunks


def build_prompt(question, chunks, compliance_data=None):
    context_blocks = []
    for i, chunk in enumerate(chunks, 1):
        context_blocks.append(
            f"[Source {i}: {chunk['source']}, {chunk['date']}]\n{chunk['text']}"
        )
    regulatory_context = "\n\n---\n\n".join(context_blocks)
    live_data_section = ""
    if compliance_data:
        live_data_section = f"\nLIVE PIPELINE DATA:\n{compliance_data}\n"

    return f"""You are a CISAF compliance advisor for German industrial companies. Answer the question using the regulatory context and live data provided. Be precise and cite which source supports each key claim. If the data does not contain enough information to answer fully, say so clearly.

IMPORTANT INTERPRETATION RULES FOR LIVE DATA:
- compliance_pct is the percentage of the reinvestment obligation already met. Values above 100% mean the facility has EXCEEDED its obligation and is fully compliant.
- compliance_status=COMPLIANT means the facility is meeting its obligations.
- compliance_status=N/A or compliance_pct=0.0 means the facility has NO recorded investment and is AT RISK.
- A facility is only at risk if compliance_pct is low (below 50%) or status is not COMPLIANT.

REGULATORY CONTEXT (retrieved from public sources):
{regulatory_context}
{live_data_section}
QUESTION: {question}

Answer in clear, direct language suitable for a compliance officer or CFO. Reference sources by name."""


def answer_question(question, use_live_data=False):
    print(f"\n{'='*65}")
    print(f"QUESTION: {question}")
    print(f"{'='*65}")

    embed_model = SentenceTransformer(EMBED_MODEL)
    nli_model = CrossEncoder(NLI_MODEL)
    chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
    collection = chroma_client.get_collection(COLLECTION_NAME)

    chunks = retrieve_chunks(question, embed_model, collection)
    print(f"\nRetrieved {len(chunks)} chunks:")
    for c in chunks:
        print(f"  [{c['similarity']:.3f}] {c['source']} -- {c['title'][:60]}...")

    compliance_text = None
    if use_live_data and DATABRICKS_TOKEN:
        print("\nFetching live compliance data from Databricks...")
        rows = get_compliance_data()
        compliance_text = format_compliance_data(rows)
        print("Live data retrieved.")

    prompt = build_prompt(question, chunks, compliance_text)

    print("\nGenerating answer via Groq API...")
    groq_client = Groq(api_key=GROQ_API_KEY)
    response = groq_client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1024,
    )
    answer = response.choices[0].message.content

    print("\nRunning hallucination gate (NLI faithfulness check)...")
    gate_result = check_faithfulness(answer, chunks, nli_model)
    print(f"  Max entailment score: {gate_result['max_entailment_score']}")
    print(f"  Best supporting chunk: {gate_result['best_supporting_chunk']}")
    print(f"  All chunk scores: {gate_result['all_scores']}")
    if gate_result["flagged"]:
        print(f"  WARNING: Answer flagged -- entailment score {gate_result['max_entailment_score']} below threshold {FAITHFULNESS_THRESHOLD}")
        print(f"  Answer served with flag. Verify against sources before use.")
    else:
        print(f"  PASSED: Answer is faithful to retrieved context.")

    print(f"\nANSWER:\n{answer}")
    print(f"\nSOURCES USED:")
    seen = set()
    for c in chunks:
        if c["url"] not in seen:
            print(f"  - {c['source']} ({c['date']}): {c['url']}")
            seen.add(c["url"])
    print(f"{'='*65}\n")
    return answer, gate_result


# ── Demo queries ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY environment variable not set.")
    if not DATABRICKS_TOKEN:
        print("WARNING: DATABRICKS_TOKEN not set. Live data queries will be skipped.")

    answer_question(
        "What qualifies as a demand flexibility investment under CISAF "
        "and what bonus does it attract?",
        use_live_data=False,
    )

    answer_question(
        "What is the reinvestment deadline under CISAF and what are "
        "the consequences if a company fails to meet it?",
        use_live_data=False,
    )

    answer_question(
        "Which of our facilities are at risk of missing the CISAF "
        "reinvestment obligation and what should they prioritize?",
        use_live_data=True,
    )
