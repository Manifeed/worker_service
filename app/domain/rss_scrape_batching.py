from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Iterable
from random import Random
from urllib.parse import urlparse

from shared_backend.schemas.rss.rss_scrape_job_schema import RssScrapeFeedPayloadSchema


def build_rss_scrape_batches(
    feeds: list[RssScrapeFeedPayloadSchema],
    *,
    batch_size: int,
    random_seed: str,
) -> list[list[RssScrapeFeedPayloadSchema]]:
    if not feeds:
        return []

    normalized_batch_size = max(1, min(batch_size, 20))
    grouped_feeds = OrderedDict[tuple[str, int], list[RssScrapeFeedPayloadSchema]]()

    for feed in feeds:
        grouping_key = _build_grouping_key(feed)
        grouped_feeds.setdefault(grouping_key, []).append(feed)

    company_batches = [
        batch
        for company_feeds in grouped_feeds.values()
        for batch in _chunked(company_feeds, normalized_batch_size)
    ]
    return _mix_batches_by_host(company_batches, random_seed=random_seed)


def _build_grouping_key(feed: RssScrapeFeedPayloadSchema) -> tuple[str, int]:
    if feed.company_id is not None:
        return ("company", feed.company_id)
    return ("feed", feed.feed_id)


def _chunked(
    items: list[RssScrapeFeedPayloadSchema],
    batch_size: int,
) -> Iterable[list[RssScrapeFeedPayloadSchema]]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def _mix_batches_by_host(
    batches: list[list[RssScrapeFeedPayloadSchema]],
    *,
    random_seed: str,
) -> list[list[RssScrapeFeedPayloadSchema]]:
    if len(batches) <= 1:
        return batches

    batches_by_host: OrderedDict[str, deque[list[RssScrapeFeedPayloadSchema]]] = OrderedDict()
    for batch in batches:
        host_key = _build_batch_host_key(batch)
        batches_by_host.setdefault(host_key, deque()).append(batch)

    host_order = list(batches_by_host.keys())
    Random(random_seed).shuffle(host_order)  # nosec

    mixed_batches: list[list[RssScrapeFeedPayloadSchema]] = []
    while True:
        emitted = False
        for host_key in host_order:
            host_batches = batches_by_host[host_key]
            if not host_batches:
                continue
            mixed_batches.append(host_batches.popleft())
            emitted = True
        if not emitted:
            return mixed_batches


def _build_batch_host_key(batch: list[RssScrapeFeedPayloadSchema]) -> str:
    first_feed = batch[0]
    if first_feed.host_header:
        return first_feed.host_header.strip().lower()

    parsed_url = urlparse(first_feed.feed_url)
    if parsed_url.netloc:
        return parsed_url.netloc.lower()

    return f"feed:{first_feed.feed_id}"
