from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5
import httpx

from app.domain.source_embedding_config import (
    resolve_qdrant_api_key,
    resolve_qdrant_collection_name,
    resolve_qdrant_url,
)

_ENSURED_COLLECTIONS: dict[str, int] = {}


def _published_at_to_unix_seconds(value: datetime | None) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        normalized = value.replace(tzinfo=UTC)
    else:
        normalized = value.astimezone(UTC)
    return int(normalized.timestamp())


def _parse_qdrant_published_at(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


def _parse_payload_feeds(raw: object) -> list[dict[str, object]]:
    if not isinstance(raw, list):
        return []
    feeds: list[dict[str, object]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        feed_id = item.get("id")
        if not isinstance(feed_id, int):
            continue
        section = item.get("section")
        feeds.append(
            {
                "id": feed_id,
                "section": (str(section) if section is not None else None),
            }
        )
    return feeds


def _parse_payload_authors(raw: object) -> list[dict[str, object]]:
    if not isinstance(raw, list):
        return []
    authors: list[dict[str, object]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        author_id = item.get("id")
        if not isinstance(author_id, int):
            continue
        name = item.get("name")
        authors.append({"id": author_id, "name": str(name) if name is not None else ""})
    return authors


class QdrantIndexingError(RuntimeError):
    """Raised when a Qdrant operation fails."""


@dataclass(frozen=True)
class QdrantArticleEmbeddingPointRead:
    point_id: str
    article_id: int | None
    article_key: str
    company_id: int | None
    company: str | None
    country: str
    published_at: datetime | None
    url: str | None
    title: str | None
    summary: str | None
    feeds: list[dict[str, object]]
    authors: list[dict[str, object]]
    img_url: str | None
    vector: list[float]


@dataclass(frozen=True)
class QdrantArticleEmbeddingPointSummaryRead:
    point_id: str
    article_id: int | None
    article_key: str | None


@dataclass(frozen=True)
class QdrantScoredArticleEmbeddingPointRead:
    point_id: str
    score: float
    article_id: int | None
    article_key: str | None
    published_at: datetime | None = None


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
        country: str | None,
        published_at: datetime | None,
        feeds: list[dict[str, object]],
        authors: list[dict[str, object]],
        img_url: str | None,
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
            "url": url,
            "title": title,
            "summary": summary,
            "company_id": company_id,
            "company": company,
            "country": _normalize_country(country),
            "published_at": _published_at_to_unix_seconds(published_at),
            "feeds": feeds,
            "authors": authors,
            "img_url": img_url,
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
        img_raw = payload.get("img_url")
        img_url = str(img_raw) if img_raw is not None else None
        return QdrantArticleEmbeddingPointRead(
            point_id=str(point.get("id") or point_id),
            article_id=(
                int(payload["article_id"])
                if payload.get("article_id") is not None
                else None
            ),
            article_key=str(payload.get("article_key") or article_key),
            company_id=(
                int(payload["company_id"])
                if payload.get("company_id") is not None
                else None
            ),
            country=_normalize_country(payload.get("country")),
            published_at=_parse_qdrant_published_at(payload.get("published_at")),
            company=(str(payload["company"]) if payload.get("company") is not None else None),
            url=(str(payload["url"]) if payload.get("url") is not None else None),
            title=(str(payload["title"]) if payload.get("title") is not None else None),
            summary=(str(payload["summary"]) if payload.get("summary") is not None else None),
            feeds=_parse_payload_feeds(payload.get("feeds")),
            authors=_parse_payload_authors(payload.get("authors")),
            img_url=img_url,
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
                published_at=_parse_qdrant_published_at(payload.get("published_at")),
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
            ("country", "keyword"),
            ("published_at", "integer"),
            ("company_id", "integer"),
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


def _normalize_country(value: object) -> str:
    if value is None:
        return "xx"
    normalized = str(value).strip().casefold()[:2]
    return normalized or "xx"
