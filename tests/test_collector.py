import os
from datetime import datetime, timedelta

from app.database import Base, SessionLocal, engine
from app.models import Job, Post
from app.services.collector import Collector, ExternalPost, SimpleSentimentAnalyzer


def setup_function():
    if os.path.exists("app.db"):
        engine.dispose()
        os.remove("app.db")
    Base.metadata.create_all(bind=engine)


class FakeClient:
    def __init__(self, name, posts):
        self.name = name
        self._posts = posts

    @property
    def configured(self):
        return True

    def fetch(self, query: str, limit: int):
        return [post for post in self._posts][:limit]


def build_fake_collector(fake_posts):
    fake_client = FakeClient("FakeSource", fake_posts)
    return Collector(
        twitter_client=fake_client,
        reddit_client=None,
        news_client=None,
        sentiment_analyzer=SimpleSentimentAnalyzer(),
    )


def test_collect_persists_posts_and_respects_limit():
    session = SessionLocal()
    job = Job(query="test", limit=2)
    session.add(job)
    session.commit()
    session.refresh(job)

    posts = [
        ExternalPost(
            content="Great progress in renewable energy",
            source="Twitter",
            collected_at=datetime.utcnow(),
            author_location="Europe",
        ),
        ExternalPost(
            content="Worried about market decline",
            source="Reddit",
            collected_at=datetime.utcnow() - timedelta(hours=1),
            author_location=None,
        ),
        ExternalPost(
            content="Neutral observation",
            source="NewsAPI",
            collected_at=datetime.utcnow() - timedelta(hours=2),
            author_location="Asia",
        ),
    ]

    collector = build_fake_collector(posts)
    collected_posts = collector.collect(session, job)

    assert len(collected_posts) == 2
    stored = session.query(Post).filter(Post.job_id == job.id).all()
    assert len(stored) == 2
    assert all(p.author_location for p in stored)
    assert all(p.sentiment_label in {"positive", "neutral", "negative"} for p in stored)
    session.close()


def test_collect_uses_fallback_location_and_sentiment():
    session = SessionLocal()
    job = Job(query="another", limit=1)
    session.add(job)
    session.commit()
    session.refresh(job)

    posts = [
        ExternalPost(
            content="This is fine",
            source="Twitter",
            collected_at=datetime.utcnow(),
            author_location=None,
        )
    ]
    collector = build_fake_collector(posts)
    collected_posts = collector.collect(session, job)

    assert collected_posts[0].author_location == "Unknown"
    assert collected_posts[0].sentiment_label in {"positive", "neutral", "negative"}
    session.close()
