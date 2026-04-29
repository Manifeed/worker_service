from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import NAMESPACE_URL, uuid5
import httpx

from app.domain.source_embedding_config import (
    resolve_qdrant_api_key,
    resolve_qdrant_collection_name,
    resolve_qdrant_url,
)

_ENSURED_COLLECTIONS: dict[str, int] = {}


class QdrantIndexingError(RuntimeError):
    """Raised when a Qdrant operation fails."""


@dataclass(frozen=True)
class QdrantArticleEmbeddingPointRead:
    point_id: str
    article_id: int | None
    article_key: str
    worker_version: str
    company_id: int | None
    company: str | None
    language: str | None
    published_at: str | None
    url: str | None
    title: str | None
    summary: str | None
    feed_ids: list[int]
    feeds: list[dict]
    author_ids: list[int]
    authors: list[str]
    images_url: list[str]
    vector: list[float]


@dataclass(frozen=True)
class QdrantArticleEmbeddingPointSummaryRead:
    point_id: str
    article_id: int | None
    article_key: str | None
    worker_version: str | None


@dataclass(frozen=True)
class QdrantScoredArticleEmbeddingPointRead:
    point_id: str
    score: float
    article_id: int | None
    article_key: str | None
    worker_version: str | None


class SimpleQdrantClient:
    def __init__(self, http_client: httpx.Client | None = None) -> None:
        self.base_url = resolve_qdrant_url()
        self.collection_name = resolve_qdrant_collection_name()
        self.api_key = resolve_qdrant_api_key()
        self._http_client = http_client

    def upsert_article_embedding(
        self,
        *,
        article_id: int,
        article_key: str,
        worker_version: str,
        vector: list[float],
        url: str,
        title: str,
        summary: str | None,
        company_id: int | None,
        company: str | None,
        language: str | None,
        published_at: datetime | None,
        feed_ids: list[int],
        feeds: list[dict],
        author_ids: list[int],
        authors: list[str],
        images_url: list[str],
    ) -> str:
        dimensions = len(vector)
        self._ensure_collection(dimensions=dimensions)
        point_id = build_article_embedding_point_id(
            article_key=article_key,
            worker_version=worker_version,
        )
        payload = {
            "article_id": article_id,
            "article_key": article_key,
            "worker_version": worker_version,
            "url": url,
            "title": title,
            "summary": summary,
            "company_id": company_id,
            "company": company,
            "language": language,
            "published_at": (
                published_at.isoformat()
                if published_at is not None
                else None
            ),
            "feed_ids": feed_ids,
            "feeds": feeds,
            "author_ids": author_ids,
            "authors": authors,
            "images_url": images_url,
        }
        response = self._request(
            method="PUT",
            path=f"/collections/{self.collection_name}/points?wait=true",
            json={
                "points": [
                    {
                        "id": point_id,
                        "vector": vector,
                        "payload": payload,
                    }
                ],
            },
        )
        self._require_qdrant_success(response, "Unable to upsert embedding point")
        return point_id

    def get_article_embedding_point(
        self,
        *,
        article_key: str,
        worker_version: str,
    ) -> QdrantArticleEmbeddingPointRead | None:
        point_id = build_article_embedding_point_id(
            article_key=article_key,
            worker_version=worker_version,
        )
        response = self._request(
            method="POST",
            path=f"/collections/{self.collection_name}/points",
            json={
                "ids": [point_id],
                "with_payload": True,
                "with_vector": True,
            },
        )
        self._require_qdrant_success(response, "Unable to read embedding point")
        points = response.json().get("result") or []
        if not points:
            return None

        point = points[0]
        payload = point.get("payload") or {}
        raw_vector = point.get("vector")
        if not isinstance(raw_vector, list):
            raise QdrantIndexingError(
                f"Unable to read embedding point {point_id}: vector missing from payload"
            )
        vector = [float(value) for value in raw_vector]
        return QdrantArticleEmbeddingPointRead(
            point_id=str(point.get("id") or point_id),
            article_id=(
                int(payload["article_id"])
                if payload.get("article_id") is not None
                else None
            ),
            article_key=str(payload.get("article_key") or article_key),
            worker_version=str(payload.get("worker_version") or worker_version),
            company_id=(
                int(payload["company_id"])
                if payload.get("company_id") is not None
                else None
            ),
            language=(
                str(payload["language"])
                if payload.get("language") is not None
                else None
            ),
            published_at=(
                str(payload["published_at"])
                if payload.get("published_at") is not None
                else None
            ),
            company=(str(payload["company"]) if payload.get("company") is not None else None),
            url=(str(payload["url"]) if payload.get("url") is not None else None),
            title=(str(payload["title"]) if payload.get("title") is not None else None),
            summary=(str(payload["summary"]) if payload.get("summary") is not None else None),
            feed_ids=[
                int(feed_id)
                for feed_id in (payload.get("feed_ids") or [])
                if isinstance(feed_id, int)
            ],
            feeds=[
                dict(feed)
                for feed in (payload.get("feeds") or [])
                if isinstance(feed, dict)
            ],
            author_ids=[
                int(author_id)
                for author_id in (payload.get("author_ids") or [])
                if isinstance(author_id, int)
            ],
            authors=[
                str(author)
                for author in (payload.get("authors") or [])
                if isinstance(author, str)
            ],
            images_url=[
                str(image_url)
                for image_url in (payload.get("images_url") or [])
                if isinstance(image_url, str)
            ],
            vector=vector,
        )

    def delete_point_ids(self, point_ids: list[str]) -> None:
        unique_point_ids = sorted({point_id for point_id in point_ids if point_id})
        if not unique_point_ids:
            return
        response = self._request(
            method="POST",
            path=f"/collections/{self.collection_name}/points/delete?wait=true",
            json={
                "points": unique_point_ids,
            },
        )
        self._require_qdrant_success(response, "Unable to delete embedding points")

    def scroll_article_embedding_points(
        self,
        *,
        limit: int,
        offset: str | None = None,
    ) -> tuple[list[QdrantArticleEmbeddingPointSummaryRead], str | None]:
        payload: dict[str, object] = {
            "limit": limit,
            "with_payload": True,
            "with_vector": False,
        }
        if offset is not None:
            payload["offset"] = offset
        response = self._request(
            method="POST",
            path=f"/collections/{self.collection_name}/points/scroll",
            json=payload,
        )
        self._require_qdrant_success(response, "Unable to scroll embedding points")
        result = response.json().get("result") or {}
        points = result.get("points") or []
        items = [
            QdrantArticleEmbeddingPointSummaryRead(
                point_id=str(point.get("id")),
                article_id=(
                    int(payload["article_id"])
                    if isinstance((payload := point.get("payload") or {}).get("article_id"), int)
                    else None
                ),
                article_key=(
                    str(payload["article_key"])
                    if payload.get("article_key") is not None
                    else None
                ),
                worker_version=(
                    str(payload["worker_version"])
                    if payload.get("worker_version") is not None
                    else None
                ),
            )
            for point in points
        ]
        next_offset = result.get("next_page_offset")
        return items, (str(next_offset) if next_offset is not None else None)

    def search_similar_article_embeddings(
        self,
        *,
        article_key: str,
        worker_version: str,
        limit: int,
    ) -> list[QdrantScoredArticleEmbeddingPointRead]:
        point_id = build_article_embedding_point_id(
            article_key=article_key,
            worker_version=worker_version,
        )
        response = self._request(
            method="POST",
            path=f"/collections/{self.collection_name}/points/recommend",
            json={
                "positive": [point_id],
                "limit": max(1, int(limit)),
                "with_payload": True,
                "with_vector": False,
            },
        )
        self._require_qdrant_success(response, "Unable to search similar embedding points")
        points = response.json().get("result") or []
        return [
            QdrantScoredArticleEmbeddingPointRead(
                point_id=str(point.get("id")),
                score=float(point.get("score") or 0.0),
                article_id=(
                    int(payload["article_id"])
                    if isinstance((payload := point.get("payload") or {}).get("article_id"), int)
                    else None
                ),
                article_key=(
                    str(payload["article_key"])
                    if payload.get("article_key") is not None
                    else None
                ),
                worker_version=(
                    str(payload["worker_version"])
                    if payload.get("worker_version") is not None
                    else None
                ),
            )
            for point in points
        ]

    def _ensure_collection(
        self,
        *,
        dimensions: int,
    ) -> None:
        cached_dimensions = _ENSURED_COLLECTIONS.get(self.collection_name)
        if cached_dimensions == dimensions:
            return

        response = self._request(
            method="GET",
            path=f"/collections/{self.collection_name}",
        )
        if response.status_code == 404:
            create_response = self._request(
                method="PUT",
                path=f"/collections/{self.collection_name}",
                json={
                    "vectors": {
                        "size": dimensions,
                        "distance": "Cosine",
                    }
                },
            )
            self._require_qdrant_success(
                create_response,
                "Unable to create Qdrant collection",
            )
            self._ensure_payload_indexes()
            _ENSURED_COLLECTIONS[self.collection_name] = dimensions
            return

        self._require_qdrant_success(response, "Unable to read Qdrant collection")
        payload = response.json().get("result", {})
        config = payload.get("config", {})
        params = config.get("params", {})
        vectors = params.get("vectors", {})
        remote_dimensions = int(vectors.get("size") or 0)
        if remote_dimensions != dimensions:
            raise QdrantIndexingError(
                "Qdrant collection dimension mismatch: "
                f"expected {dimensions}, found {remote_dimensions}"
            )
        self._ensure_payload_indexes()
        _ENSURED_COLLECTIONS[self.collection_name] = dimensions

    def rebuild_collection(self, *, dimensions: int) -> None:
        delete_response = self._request(
            method="DELETE",
            path=f"/collections/{self.collection_name}",
        )
        if delete_response.status_code != 404:
            self._require_qdrant_success(delete_response, "Unable to delete Qdrant collection")
        _ENSURED_COLLECTIONS.pop(self.collection_name, None)
        create_response = self._request(
            method="PUT",
            path=f"/collections/{self.collection_name}",
            json={
                "vectors": {
                    "size": dimensions,
                    "distance": "Cosine",
                }
            },
        )
        self._require_qdrant_success(create_response, "Unable to recreate Qdrant collection")
        self._ensure_payload_indexes()
        _ENSURED_COLLECTIONS[self.collection_name] = dimensions

    def _ensure_payload_indexes(self) -> None:
        for field_name, field_schema in (
            ("language", "keyword"),
            ("published_at", "datetime"),
            ("feed_ids", "integer"),
            ("company_id", "integer"),
            ("author_ids", "integer"),
        ):
            response = self._request(
                method="PUT",
                path=f"/collections/{self.collection_name}/index",
                json={
                    "field_name": field_name,
                    "field_schema": field_schema,
                },
            )
            self._require_qdrant_success(
                response,
                f"Unable to create Qdrant payload index for {field_name}",
            )

    def _request(
        self,
        *,
        method: str,
        path: str,
        json: dict | None = None,
    ) -> httpx.Response:
        if self._http_client is not None:
            return self._http_client.request(
                method=method,
                url=f"{self.base_url}{path}",
                json=json,
                headers=self._build_headers(),
            )
        with httpx.Client(timeout=20.0) as client:
            return client.request(
                method=method,
                url=f"{self.base_url}{path}",
                json=json,
                headers=self._build_headers(),
            )

    def _build_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
        }
        if self.api_key is not None:
            headers["api-key"] = self.api_key
        return headers

    def _require_qdrant_success(
        self,
        response: httpx.Response,
        message: str,
    ) -> None:
        if response.status_code >= 400:
            raise QdrantIndexingError(
                f"{message}: HTTP {response.status_code} - {response.text}"
            )
        payload = response.json()
        if payload.get("status") not in (None, "ok"):
            raise QdrantIndexingError(
                f"{message}: unexpected Qdrant payload {payload}"
            )


def build_article_embedding_point_id(
    *,
    article_key: str,
    worker_version: str,
) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            f"{article_key}:{worker_version}",
        )
    )
