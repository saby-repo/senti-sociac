import random
from datetime import datetime, timedelta
from typing import List

from sqlalchemy.orm import Session

from ..models import Job, Post

POSITIVE_PHRASES = [
    "excited about",
    "loving",
    "great news on",
    "impressed by",
    "celebrating",
]

NEGATIVE_PHRASES = [
    "frustrated with",
    "concerned about",
    "disappointed in",
    "critical of",
    "worried over",
]

NEUTRAL_PHRASES = [
    "reading about",
    "observing",
    "analysing",
    "tracking",
    "monitoring",
]

SOURCES = ["Twitter", "Reddit", "News", "Blog", "YouTube"]
LOCATIONS = [
    "North America",
    "Europe",
    "Asia",
    "South America",
    "Africa",
    "Oceania",
]


class Collector:
    def __init__(self, seed: int = 42):
        random.seed(seed)

    def _build_sentence(self, query: str, sentiment: str) -> str:
        if sentiment == "positive":
            phrase = random.choice(POSITIVE_PHRASES)
        elif sentiment == "negative":
            phrase = random.choice(NEGATIVE_PHRASES)
        else:
            phrase = random.choice(NEUTRAL_PHRASES)
        return f"People are {phrase} {query} today."

    def _score_for_label(self, label: str) -> float:
        mapping = {"positive": 0.8, "neutral": 0.0, "negative": -0.6}
        noise = random.uniform(-0.2, 0.2)
        return mapping[label] + noise

    def collect(self, session: Session, job: Job) -> List[Post]:
        posts: List[Post] = []
        now = datetime.utcnow()
        for i in range(job.limit):
            label = random.choices(
                population=["positive", "neutral", "negative"],
                weights=[0.4, 0.4, 0.2],
                k=1,
            )[0]
            content = self._build_sentence(job.query, label)
            source = random.choice(SOURCES)
            location = random.choice(LOCATIONS)
            collected_at = now - timedelta(minutes=i % 1440)
            post = Post(
                job_id=job.id,
                source=source,
                author_location=location,
                content=content,
                collected_at=collected_at,
                sentiment_label=label,
                sentiment_score=self._score_for_label(label),
            )
            session.add(post)
            posts.append(post)
        session.commit()
        return posts
