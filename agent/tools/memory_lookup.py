"""
Tool: searches Qdrant for past incidents whose postmortems (written by
postmortem.py) are similar to the current one, using sentence-transformer
embeddings. Gives the `investigate` node prior art — "this looks like
the schema_drift incident from 3 days ago, here's how it was resolved."

Also exposes the embedding model, Qdrant client, and collection setup
as shared functions, since postmortem.py needs to write to the exact
same collection this reads from.
"""

import os
from functools import lru_cache

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams
from sentence_transformers import SentenceTransformer

QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")
QDRANT_API_KEY = os.environ.get("QDRANT_API_KEY")  # required for Qdrant Cloud; unset/None for a local container

COLLECTION_NAME = "pipeline_incidents"
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384


@lru_cache(maxsize=1)
def get_embedding_model() -> SentenceTransformer:
    return SentenceTransformer(EMBEDDING_MODEL_NAME)


@lru_cache(maxsize=1)
def get_qdrant_client() -> QdrantClient:
    return QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)


def embed_text(text: str) -> list:
    vector = get_embedding_model().encode(text, normalize_embeddings=True)
    return vector.tolist()


def ensure_collection() -> None:
    client = get_qdrant_client()
    existing = {c.name for c in client.get_collections().collections}
    if COLLECTION_NAME not in existing:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE),
        )


def search_similar_incidents(query_text: str, top_k: int = 3) -> list:
    """Returns up to top_k past incidents ranked by embedding similarity
    to query_text, most similar first."""
    ensure_collection()
    client = get_qdrant_client()
    query_vector = embed_text(query_text)

    hits = client.search(
        collection_name=COLLECTION_NAME,
        query_vector=query_vector,
        limit=top_k,
    )

    return [
        {
            "score": hit.score,
            "incident_id": hit.payload.get("incident_id"),
            "error_type": hit.payload.get("error_type"),
            "summary": hit.payload.get("summary"),
            "root_cause": hit.payload.get("root_cause"),
            "resolution": hit.payload.get("resolution"),
            "created_at": hit.payload.get("created_at"),
        }
        for hit in hits
    ]
