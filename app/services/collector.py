import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, List, Optional

try:
    import praw
    from praw.models import Submission
except ImportError:  # pragma: no cover - optional dependency
    praw = None  # type: ignore
    Submission = Any  # type: ignore
import requests
from sqlalchemy.orm import Session

RedditClientType = Any if praw is None else praw.Reddit

from ..models import Job, Post


@dataclass
class ExternalPost:
    content: str
    source: str
    collected_at: datetime
    author_location: Optional[str] = None
    sentiment_label: Optional[str] = None
    sentiment_score: Optional[float] = None


class SimpleSentimentAnalyzer:
    def __init__(self):
        self.positive_words = {
            "good",
            "great",
            "excellent",
            "love",
            "amazing",
            "excited",
            "celebrate",
            "growth",
        }
        self.negative_words = {
            "bad",
            "terrible",
            "awful",
            "hate",
            "concern",
            "worried",
            "decline",
            "risk",
        }

    def evaluate(
        self,
        text: str,
        existing_label: Optional[str] = None,
        existing_score: Optional[float] = None,
    ) -> tuple[str, float]:
        if existing_label and existing_score is not None:
            return existing_label, existing_score

        tokens = re.findall(r"[a-zA-Z']+", text.lower())
        score = 0
        for token in tokens:
            if token in self.positive_words:
                score += 1
            if token in self.negative_words:
                score -= 1
        normalized = score / max(len(tokens), 1)
        if normalized > 0.05:
            label = "positive"
        elif normalized < -0.05:
            label = "negative"
        else:
            label = "neutral"
        return label, normalized


class TwitterClient:
    SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"

    def __init__(self, bearer_token: Optional[str], cooldown_seconds: int = 2):
        self.bearer_token = bearer_token
        self.cooldown_seconds = cooldown_seconds

    @classmethod
    def from_env(cls) -> "TwitterClient":
        return cls(os.getenv("TWITTER_BEARER_TOKEN"))

    @property
    def configured(self) -> bool:
        return bool(self.bearer_token)

    def fetch(self, query: str, limit: int) -> List[ExternalPost]:
        if not self.configured or limit <= 0:
            return []

        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        params = {
            "query": query,
            "max_results": min(100, limit),
            "tweet.fields": "created_at,geo,author_id",
            "expansions": "author_id",
            "user.fields": "location",
        }

        collected: List[ExternalPost] = []
        next_token: Optional[str] = None
        remaining = limit

        while remaining > 0:
            if next_token:
                params["next_token"] = next_token
            response = requests.get(self.SEARCH_URL, headers=headers, params=params, timeout=10)
            if response.status_code == 429:
                time.sleep(self.cooldown_seconds)
                continue
            response.raise_for_status()
            payload = response.json()

            users = {u.get("id"): u for u in payload.get("includes", {}).get("users", [])}
            for tweet in payload.get("data", []):
                author = users.get(tweet.get("author_id"), {})
                collected.append(
                    ExternalPost(
                        content=tweet.get("text", ""),
                        source="Twitter",
                        collected_at=self._parse_timestamp(tweet.get("created_at")),
                        author_location=author.get("location"),
                    )
                )
                remaining -= 1
                if remaining <= 0:
                    break

            next_token = payload.get("meta", {}).get("next_token")
            if not next_token:
                break

        return collected

    def _parse_timestamp(self, timestamp: Optional[str]) -> datetime:
        if not timestamp:
            return datetime.utcnow()
        try:
            return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            return datetime.utcnow()


class RedditClient:
    def __init__(self, client: Optional[RedditClientType]):
        self.client = client if praw is not None else None

    @classmethod
    def from_env(cls) -> "RedditClient":
        if praw is None:
            return cls(None)
        client_id = os.getenv("REDDIT_CLIENT_ID")
        client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        user_agent = os.getenv("REDDIT_USER_AGENT")
        if client_id and client_secret and user_agent:
            reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent,
                check_for_async=False,
            )
            return cls(reddit)
        return cls(None)

    @property
    def configured(self) -> bool:
        return self.client is not None

    def fetch(self, query: str, limit: int) -> List[ExternalPost]:
        if not self.configured or limit <= 0:
            return []

        collected: List[ExternalPost] = []
        search: Iterable[Submission] = self.client.subreddit("all").search(
            query, limit=limit, sort="new"
        )
        for submission in search:
            collected.append(
                ExternalPost(
                    content=submission.selftext or submission.title,
                    source="Reddit",
                    collected_at=datetime.utcfromtimestamp(submission.created_utc),
                    author_location=getattr(submission.author, "flair", None) or None,
                )
            )
            if len(collected) >= limit:
                break
        return collected


class NewsAPIClient:
    ENDPOINT = "https://newsapi.org/v2/everything"

    def __init__(self, api_key: Optional[str]):
        self.api_key = api_key

    @classmethod
    def from_env(cls) -> "NewsAPIClient":
        return cls(os.getenv("NEWSAPI_KEY"))

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    def fetch(self, query: str, limit: int) -> List[ExternalPost]:
        if not self.configured or limit <= 0:
            return []

        collected: List[ExternalPost] = []
        page = 1
        remaining = limit
        page_size = min(100, remaining)

        while remaining > 0:
            params = {
                "q": query,
                "apiKey": self.api_key,
                "pageSize": min(100, remaining, page_size),
                "page": page,
                "sortBy": "publishedAt",
                "language": "en",
            }
            response = requests.get(self.ENDPOINT, params=params, timeout=10)
            if response.status_code == 429:
                time.sleep(1)
                continue
            response.raise_for_status()
            payload = response.json()
            articles = payload.get("articles", [])
            if not articles:
                break

            for article in articles:
                collected.append(
                    ExternalPost(
                        content=article.get("description")
                        or article.get("content")
                        or article.get("title", ""),
                        source="NewsAPI",
                        collected_at=self._parse_timestamp(article.get("publishedAt")),
                        author_location=None,
                    )
                )
                remaining -= 1
                if remaining <= 0:
                    break
            if len(articles) < params["pageSize"]:
                break
            page += 1

        return collected

    def _parse_timestamp(self, timestamp: Optional[str]) -> datetime:
        if not timestamp:
            return datetime.utcnow()
        try:
            return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            return datetime.utcnow()


class Collector:
    def __init__(
        self,
        twitter_client: Optional[TwitterClient] = None,
        reddit_client: Optional[RedditClient] = None,
        news_client: Optional[NewsAPIClient] = None,
        sentiment_analyzer: Optional[SimpleSentimentAnalyzer] = None,
    ):
        self.twitter_client = twitter_client or TwitterClient.from_env()
        self.reddit_client = reddit_client or RedditClient.from_env()
        self.news_client = news_client or NewsAPIClient.from_env()
        self.sentiment_analyzer = sentiment_analyzer or SimpleSentimentAnalyzer()

    def _normalize_post(self, post: ExternalPost) -> ExternalPost:
        label, score = self.sentiment_analyzer.evaluate(
            post.content, post.sentiment_label, post.sentiment_score
        )
        post.sentiment_label = label
        post.sentiment_score = score
        post.author_location = post.author_location or "Unknown"
        return post

    def collect(self, session: Session, job: Job) -> List[Post]:
        posts: List[Post] = []
        remaining = job.limit
        sources = [self.twitter_client, self.reddit_client, self.news_client]

        for client in sources:
            if remaining <= 0:
                break
            if not client or not client.configured:
                continue
            for external in client.fetch(job.query, remaining):
                normalized = self._normalize_post(external)
                post = Post(
                    job_id=job.id,
                    source=normalized.source,
                    author_location=normalized.author_location,
                    content=normalized.content,
                    collected_at=normalized.collected_at,
                    sentiment_label=normalized.sentiment_label,
                    sentiment_score=normalized.sentiment_score,
                )
                session.add(post)
                posts.append(post)
                remaining -= 1
                if remaining <= 0:
                    break

        if not posts:
            raise ValueError(
                "No posts collected. Ensure API credentials are configured and the query returns data."
            )

        session.commit()
        return posts
