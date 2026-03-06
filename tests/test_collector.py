from datetime import datetime, timezone, timedelta

from app.models import Job, Post
from app.services.collector import (
    Collector,
    HackerNewsClient,
    NewsRSSClient,
    RawPost,
    RedditClient,
    SimpleSentimentAnalyzer,
    SubredditRouter,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _raw(title="Sample post", body="", source="Reddit/r/news",
         offset_hours=0, url="https://example.com"):
    ts = datetime.now(timezone.utc) - timedelta(hours=offset_hours)
    return RawPost(
        title=title, body=body, author="test_user", source=source,
        created_utc=ts.timestamp(), url=url,
    )


class FakeRedditClient:
    configured = True
    def __init__(self, posts): self._posts = posts
    def fetch_subreddit(self, sub, query, limit, sort="new"):
        return self._posts[:limit]


class FakeHNClient:
    configured = True
    def __init__(self, posts): self._posts = posts
    def fetch(self, query, limit):
        return self._posts[:limit]


class FakeNewsRSSClient:
    configured = True
    def __init__(self, posts, feeds=None):
        self._posts = posts
        self.feeds = feeds or [("FakeNews", "http://fake.rss")]
    def fetch_all(self, query, limit_per_feed):
        return self._posts[:limit_per_feed * len(self.feeds)]


def build_collector(reddit=None, hn=None, rss=None, subreddits=None):
    router = SubredditRouter()
    if subreddits is not None:
        router.route = lambda q: ("test", subreddits)
    return Collector(
        reddit_client=FakeRedditClient(reddit or []),
        hn_client=FakeHNClient(hn or []),
        news_rss_client=FakeNewsRSSClient(rss or []),
        router=router,
        sentiment_analyzer=SimpleSentimentAnalyzer(),
    )


# ── SubredditRouter ───────────────────────────────────────────────────────────

def test_router_news():
    cat, subs = SubredditRouter().route("breaking news headline")
    assert cat == "news" and "news" in subs

def test_router_technology():
    cat, subs = SubredditRouter().route("new programming language release")
    assert cat == "technology"

def test_router_ai():
    cat, _ = SubredditRouter().route("ChatGPT and LLM performance")
    assert cat == "artificial_intelligence"

def test_router_finance():
    cat, _ = SubredditRouter().route("stock market crash and inflation")
    assert cat == "finance"

def test_router_crypto():
    cat, _ = SubredditRouter().route("bitcoin price ethereum")
    assert cat == "cryptocurrency"

def test_router_sports():
    cat, _ = SubredditRouter().route("NBA finals basketball championship")
    assert cat == "sports"

def test_router_fallback():
    cat, subs = SubredditRouter().route("zzz_unknown_xyz")
    assert cat == "general" and subs == ["all"]

def test_router_environment():
    cat, _ = SubredditRouter().route("climate change renewable energy solar")
    assert cat == "environment"


# ── NewsRSSClient helpers ─────────────────────────────────────────────────────

def test_rss_strip_html():
    from app.services.collector import _strip_html
    assert _strip_html("<p>Hello <b>world</b></p>") == "Hello world"

def test_rss_parse_rfc2822_valid():
    from app.services.collector import _parse_rfc2822
    assert _parse_rfc2822("Thu, 05 Mar 2026 11:15:09 +0530") > 0

def test_rss_parse_rfc2822_empty():
    from app.services.collector import _parse_rfc2822
    assert _parse_rfc2822("") > 0

def test_rss_configured_default():
    assert NewsRSSClient().configured is True

def test_rss_empty_feeds_not_configured():
    assert NewsRSSClient(feeds=[]).configured is False


# ── HackerNewsClient ──────────────────────────────────────────────────────────

def test_hn_client_configured():
    assert HackerNewsClient().configured is True


# ── Collector integration ─────────────────────────────────────────────────────

def test_collect_merges_all_sources(db_session):
    reddit_posts = [_raw("Reddit post", source="Reddit/r/news")]
    hn_posts     = [_raw("HN story",   source="Hacker News", url="https://hn.com/1")]
    rss_posts    = [_raw("TOI article",source="Times of India")]
    collector = build_collector(reddit_posts, hn_posts, rss_posts, subreddits=["news"])

    job = Job(query="news", limit=100)
    db_session.add(job); db_session.commit(); db_session.refresh(job)

    posts = collector.collect(db_session, job)
    sources = {p.source for p in posts}
    assert "Reddit/r/news" in sources
    assert "Hacker News" in sources
    assert "Times of India" in sources


def test_collect_persists_url(db_session):
    hn_posts = [_raw("Story with link", source="Hacker News",
                     url="https://news.ycombinator.com/item?id=123")]
    collector = build_collector(hn=hn_posts, subreddits=["news"])

    job = Job(query="tech", limit=10)
    db_session.add(job); db_session.commit(); db_session.refresh(job)

    posts = collector.collect(db_session, job)
    assert posts[0].url == "https://news.ycombinator.com/item?id=123"


def test_collect_respects_limit(db_session):
    many = [_raw(f"Post {i}", source="Reddit/r/news") for i in range(30)]
    collector = build_collector(many, subreddits=["news"])

    job = Job(query="news", limit=10)
    db_session.add(job); db_session.commit(); db_session.refresh(job)

    posts = collector.collect(db_session, job)
    assert len(posts) <= 10


def test_collect_deduplicates(db_session):
    # Same title from two sources → stored once
    dup_title = "Exact same headline"
    posts_in = [
        RawPost(title=dup_title, body="", author="u1", source="Hacker News",
                created_utc=datetime.now(timezone.utc).timestamp(), url="https://a.com"),
        RawPost(title=dup_title, body="", author="TOI", source="Times of India",
                created_utc=datetime.now(timezone.utc).timestamp(), url="https://b.com"),
    ]
    collector = build_collector(rss=posts_in, subreddits=["news"])

    job = Job(query="news", limit=100)
    db_session.add(job); db_session.commit(); db_session.refresh(job)

    posts = collector.collect(db_session, job)
    assert len(posts) == 1


def test_collect_raises_when_empty(db_session):
    import pytest
    collector = build_collector(subreddits=["news"])
    job = Job(query="xyzzy", limit=5)
    db_session.add(job); db_session.commit(); db_session.refresh(job)
    with pytest.raises(ValueError, match="No posts collected"):
        collector.collect(db_session, job)


def test_collect_works_rss_only(db_session):
    rss = [_raw(f"Article {i}", source="The Hindu") for i in range(5)]
    collector = build_collector(rss=rss, subreddits=["news"])
    job = Job(query="india", limit=10)
    db_session.add(job); db_session.commit(); db_session.refresh(job)
    posts = collector.collect(db_session, job)
    assert all(p.source == "The Hindu" for p in posts)


# ── Sentiment ─────────────────────────────────────────────────────────────────

def test_sentiment_positive():
    label, score = SimpleSentimentAnalyzer().evaluate("Great amazing excellent win")
    assert label == "positive" and score > 0

def test_sentiment_negative():
    label, score = SimpleSentimentAnalyzer().evaluate("terrible disaster awful failure")
    assert label == "negative" and score < 0

def test_sentiment_negation():
    sa = SimpleSentimentAnalyzer()
    _, pos = sa.evaluate("This is great")
    _, neg = sa.evaluate("This is not great")
    assert pos > 0 and neg <= 0

def test_sentiment_neutral():
    label, _ = SimpleSentimentAnalyzer().evaluate("the cat sat on the mat")
    assert label == "neutral"
