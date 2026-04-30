from __future__ import annotations

from typing import TYPE_CHECKING

from arxiv_demo.gvecdb_client import pack_float32_vector

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer

MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

_model: SentenceTransformer | None = None


def get_model() -> SentenceTransformer:
    global _model  # noqa: PLW0603
    if _model is None:
        from sentence_transformers import SentenceTransformer

        _model = SentenceTransformer(MODEL_NAME)
    return _model


def embed_texts(texts: list[str]) -> list[list[float]]:
    model = get_model()
    embeddings = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
    return [row.tolist() for row in embeddings]


def embed_single(text: str) -> list[float]:
    return embed_texts([text])[0]


def embed_to_bytes(text: str) -> bytes:
    return pack_float32_vector(embed_single(text))


def embed_batch_to_bytes(texts: list[str]) -> list[bytes]:
    vecs = embed_texts(texts)
    return [pack_float32_vector(v) for v in vecs]
