from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.services.article_language_backfill_service import backfill_unknown_article_languages
from app.services.rss_worker_article_persistence import (
    _update_article_from_candidate,
    _upsert_article,
)
from app.services.rss_worker_ingestion_candidates import (
    CandidateRow,
    FeedContext,
    build_candidate_rows,
    upsert_article_url_variants,
)
from app.schemas.workers.worker_result_schema import WorkerResultSchema, WorkerResultSourceSchema


class FakeFastTextDetector:
    def predict(self, text: str, k: int = 1):
        if "Bundestag" in text:
            return ["__label__de"], [0.98]
        return ["__label__en"], [0.91]


def _create_session() -> Session:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    session = sessionmaker(bind=engine, future=True)()
    session.execute(
        text(
            """
            CREATE TABLE articles (
                article_id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_key TEXT NOT NULL,
                content_key TEXT,
                published_at TEXT,
                ingested_at TEXT,
                canonical_url TEXT,
                title TEXT,
                summary TEXT,
                image_url TEXT,
                country TEXT NOT NULL DEFAULT 'xx',
                language TEXT NOT NULL DEFAULT 'xx',
                company_id INTEGER
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE article_url (
                article_id INTEGER NOT NULL,
                url TEXT PRIMARY KEY,
                first_seen_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    )
    return session


def _build_candidate(*, language: str) -> CandidateRow:
    return CandidateRow(
        feed_id=1,
        article_key="article-key",
        content_key="content-key",
        published_at=datetime(2026, 1, 1, tzinfo=UTC),
        canonical_url="https://example.com/article",
        source_urls=("https://example.com/article?utm_source=rss",),
        title="Titre",
        summary="Resume",
        authors=("Ada",),
        image_url="https://example.com/image.png",
        company_id=7,
        country="fr",
        language=language,
        duplicate_hint=False,
    )


def test_build_candidate_rows_sets_detected_language(monkeypatch) -> None:
    monkeypatch.setattr("app.services.rss_worker_ingestion_candidates._load_existing_article_keys", lambda *_args, **_kwargs: set())
    monkeypatch.setattr("app.services.rss_worker_ingestion_candidates._load_existing_content_keys", lambda *_args, **_kwargs: set())
    monkeypatch.setattr("app.services.rss_worker_ingestion_candidates.detect_article_language", lambda **_kwargs: "de")

    results = [
        WorkerResultSchema(
            feed_id=1,
            job_id="job",
            status="success",
            sources=[
                WorkerResultSourceSchema(
                    title="Bundestag stimmt ab",
                    summary="Berlin",
                    urls=["https://example.com/a?utm_source=rss"],
                )
            ],
        )
    ]

    rows = build_candidate_rows(
        object(),  # type: ignore[arg-type]
        feed_contexts={
            1: FeedContext(
                feed_id=1,
                feed_url="https://www.consilium.europa.eu/en/rss/pressreleases.ashx",
                company_id=7,
                company_name="Example",
                country="eu",
            )
        },
        results=results,
    )

    assert len(rows) == 1
    assert rows[0].language == "de"


def test_upsert_article_inserts_language_on_new_article() -> None:
    session = _create_session()

    article_id, _article_key = _upsert_article(session, candidate_row=_build_candidate(language="fr"))
    session.commit()

    row = session.execute(
        text("SELECT article_id, language FROM articles WHERE article_id = :article_id"),
        {"article_id": article_id},
    ).mappings().one()
    assert int(row["article_id"]) == article_id
    assert str(row["language"]) == "fr"


def test_update_article_from_candidate_does_not_overwrite_known_language() -> None:
    session = _create_session()
    session.execute(
        text(
            """
            INSERT INTO articles (
                article_id, article_key, content_key, canonical_url, title, summary, country, language, company_id
            ) VALUES (
                1, 'article-key', 'content-key', 'https://example.com/article', 'Old', 'Old summary', 'be', 'en', 7
            )
            """
        )
    )

    _update_article_from_candidate(session, article_id=1, candidate_row=_build_candidate(language="fr"))
    session.commit()

    row = session.execute(text("SELECT language FROM articles WHERE article_id = 1")).mappings().one()
    assert str(row["language"]) == "en"


def test_update_article_from_candidate_enriches_unknown_language() -> None:
    session = _create_session()
    session.execute(
        text(
            """
            INSERT INTO articles (
                article_id, article_key, content_key, canonical_url, title, summary, country, language, company_id
            ) VALUES (
                1, 'article-key', 'content-key', 'https://example.com/article', 'Old', 'Old summary', 'be', 'xx', 7
            )
            """
        )
    )

    _update_article_from_candidate(session, article_id=1, candidate_row=_build_candidate(language="nl"))
    session.commit()

    row = session.execute(text("SELECT language FROM articles WHERE article_id = 1")).mappings().one()
    assert str(row["language"]) == "nl"


def test_upsert_article_url_variants_stores_only_normalized_urls() -> None:
    session = _create_session()

    upsert_article_url_variants(
        session,
        article_id=9,
        raw_urls=(
            "https://example.com/article?utm_source=rss&id=1",
            "https://example.com/article?id=1",
        ),
    )
    session.commit()

    rows = session.execute(text("SELECT article_id, url FROM article_url ORDER BY url")).mappings().all()
    assert rows == [{"article_id": 9, "url": "https://example.com/article?id=1"}]


def test_backfill_unknown_article_languages_updates_only_detected_rows() -> None:
    session = _create_session()
    session.execute(
        text(
            """
            INSERT INTO articles (article_id, article_key, title, summary, country, language)
            VALUES
                (1, 'a', 'Bundestag stimmt ab', 'Berlin', 'eu', 'xx'),
                (2, 'b', '', NULL, 'be', 'xx'),
                (3, 'c', 'Already set', NULL, 'us', 'en')
            """
        )
    )

    batch = backfill_unknown_article_languages(
        session,
        batch_size=10,
        detector=FakeFastTextDetector(),
    )
    session.commit()

    rows = session.execute(text("SELECT article_id, language FROM articles ORDER BY article_id")).mappings().all()
    assert batch.scanned_count == 2
    assert batch.updated_count == 1
    assert batch.last_article_id == 2
    assert rows == [
        {"article_id": 1, "language": "de"},
        {"article_id": 2, "language": "xx"},
        {"article_id": 3, "language": "en"},
    ]
