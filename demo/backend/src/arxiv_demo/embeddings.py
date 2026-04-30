from __future__ import annotations

import json
import os

import boto3

from arxiv_demo.gvecdb_client import pack_float32_vector

BEDROCK_MODEL_ID = "cohere.embed-english-v3"
BEDROCK_REGION = os.environ.get("AWS_REGION", "eu-west-1")
EMBEDDING_DIM = 1024
BEDROCK_BATCH_LIMIT = 96

_client: boto3.client | None = None


def _get_client() -> boto3.client:
    global _client  # noqa: PLW0603
    if _client is None:
        _client = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)
    return _client


def _invoke_cohere(texts: list[str], input_type: str) -> list[list[float]]:
    client = _get_client()
    all_embeddings: list[list[float]] = []
    for i in range(0, len(texts), BEDROCK_BATCH_LIMIT):
        batch = texts[i : i + BEDROCK_BATCH_LIMIT]
        body = json.dumps({
            "texts": batch,
            "input_type": input_type,
            "truncate": "END",
        })
        response = client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=body,
        )
        result = json.loads(response["body"].read())
        all_embeddings.extend(result["embeddings"])
    return all_embeddings


def embed_texts(texts: list[str], input_type: str = "search_document") -> list[list[float]]:
    return _invoke_cohere(texts, input_type)


def embed_single(text: str, input_type: str = "search_document") -> list[float]:
    return embed_texts([text], input_type=input_type)[0]


def embed_to_bytes(text: str, input_type: str = "search_document") -> bytes:
    return pack_float32_vector(embed_single(text, input_type=input_type))


def embed_query_to_bytes(text: str) -> bytes:
    return embed_to_bytes(text, input_type="search_query")


def embed_batch_to_bytes(texts: list[str], input_type: str = "search_document") -> list[bytes]:
    vecs = embed_texts(texts, input_type=input_type)
    return [pack_float32_vector(v) for v in vecs]
