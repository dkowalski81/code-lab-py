"""
Hybrid RAG with Qdrant — dense + sparse (BM42) search + conversational memory.

Setup:
    pip install qdrant-client[fastembed] openai

Run:
    python rag.py
"""

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue
from openai import OpenAI

# ── SETUP ─────────────────────────────────────────────────────────────────────

qdrant = QdrantClient(path="./qdrant_data")
qdrant.set_model("BAAI/bge-small-en-v1.5")                          # dense (semantic)
qdrant.set_sparse_model("Qdrant/bm42-all-minilm-l6-v2-attentions") # sparse (keyword)
# add() and query() now automatically use both → RRF fusion

openai_client = OpenAI()  # reads OPENAI_API_KEY from environment
COLLECTION = "docs"

# ── CHUNKING ──────────────────────────────────────────────────────────────────

def chunk_text(text: str, chunk_size: int = 300, overlap: int = 50) -> list[str]:
    chunks, start = [], 0
    while start < len(text):
        chunks.append(text[start:start + chunk_size])
        start += chunk_size - overlap
    return chunks

# ── DOCUMENTS (replace with your own) ────────────────────────────────────────

documents = [
    {
        "text": "Azure Container Apps is a serverless platform built on Kubernetes. "
                "It supports automatic scaling via KEDA scalers including HTTP, queue length, "
                "and CPU. Replicas scale from zero to a configured maximum. "
                "The HTTP scaler triggers based on concurrent requests per replica. "
                "Scale-out helps throughput but not latency of individual requests. " * 5,
        "source": "azure_docs",
        "category": "cloud",
    },
    {
        "text": "Qdrant is a vector similarity search engine written in Rust. "
                "It supports filtering on payload fields, named vectors, "
                "and sparse vectors for hybrid search using RRF fusion. "
                "Collections store points consisting of vectors and optional payloads. "
                "FastEmbed integration handles embedding generation automatically. " * 5,
        "source": "qdrant_docs",
        "category": "database",
    },
    {
        "text": "Azure Document Intelligence (formerly Form Recognizer) extracts text "
                "and structure from documents using OCR. The S0 tier supports 15 "
                "transactions per second as a submission rate limit. Each OCR call "
                "can take 30-90 seconds to complete. The async API returns a job ID "
                "that you poll for results. " * 5,
        "source": "docintelligence_docs",
        "category": "ai",
    },
]

# ── INDEXING (runs once, skipped if collection already exists) ────────────────

if not qdrant.collection_exists(COLLECTION):
    print("Indexing documents...")
    chunks, metadata = [], []
    for doc in documents:
        for chunk in chunk_text(doc["text"]):
            chunks.append(chunk)
            metadata.append({"source": doc["source"], "category": doc["category"]})
    qdrant.add(collection_name=COLLECTION, documents=chunks, metadata=metadata)
    print(f"Indexed {len(chunks)} chunks.\n")
else:
    print("Collection already exists, skipping indexing.\n")

# ── CONVERSATIONAL RAG ────────────────────────────────────────────────────────

class RAGChat:
    def __init__(self):
        self.history: list[dict] = []

    def _rewrite_query(self, question: str) -> str:
        """Make follow-up questions standalone so retrieval works correctly."""
        if not self.history:
            return question
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content":
                    "Rewrite the follow-up question as a fully standalone question "
                    "using the conversation history. Return only the rewritten question."},
                *self.history[-4:],  # last 2 turns
                {"role": "user", "content": question},
            ],
        )
        return response.choices[0].message.content

    def ask(self, question: str, category: str = None) -> str:
        # 1. rewrite for better retrieval on follow-ups
        search_query = self._rewrite_query(question)
        if search_query != question:
            print(f"  [rewritten] {search_query}")

        # 2. hybrid retrieve (dense + sparse, auto-fused via RRF)
        results = qdrant.query(
            collection_name=COLLECTION,
            query_text=search_query,
            limit=3,
            query_filter=Filter(
                must=[FieldCondition(key="category", match=MatchValue(value=category))]
            ) if category else None,
        )

        sources = list({r.metadata["source"] for r in results})
        context = "\n\n".join(
            f"[{r.metadata['source']}]\n{r.document}" for r in results
        )

        # 3. generate with conversation history
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content":
                    f"Answer using only this context. Cite sources.\n\n{context}"},
                *self.history[-6:],  # last 3 turns
                {"role": "user", "content": question},
            ],
        )
        answer = response.choices[0].message.content

        # 4. persist turn in memory
        self.history.extend([
            {"role": "user",      "content": question},
            {"role": "assistant", "content": answer},
        ])

        print(f"  [sources] {', '.join(sources)}")
        return answer


# ── INTERACTIVE LOOP ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    chat = RAGChat()
    print("RAG Chat ready. Type 'quit' to exit, or prefix with 'cloud:', 'database:', 'ai:' to filter by category.\n")

    while True:
        user_input = input("You: ").strip()
        if not user_input or user_input.lower() == "quit":
            break

        # optional category filter: "cloud: how does scaling work?"
        category = None
        for cat in ("cloud", "database", "ai"):
            if user_input.lower().startswith(f"{cat}:"):
                category = cat
                user_input = user_input[len(cat) + 1:].strip()
                break

        answer = chat.ask(user_input, category=category)
        print(f"\nAssistant: {answer}\n")
