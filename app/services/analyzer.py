import base64
import io
from collections import Counter
from datetime import datetime
from typing import Dict, List

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy.orm import Session

from ..models import Analysis, Job, Post


class Analyzer:
    def __init__(self):
        pass

    def analyze(self, session: Session, job: Job, posts: List[Post]) -> Analysis:
        if not posts:
            raise ValueError("No posts to analyze")

        sentiments = Counter([p.sentiment_label for p in posts])
        locations = Counter([p.author_location for p in posts])
        sources = Counter([p.source for p in posts])
        scores = [p.sentiment_score for p in posts]
        days = Counter([p.collected_at.strftime("%Y-%m-%d") for p in posts])

        analysis = Analysis(job_id=job.id)
        analysis.total_count = len(posts)
        analysis.positive_count = sentiments.get("positive", 0)
        analysis.negative_count = sentiments.get("negative", 0)
        analysis.neutral_count = sentiments.get("neutral", 0)
        analysis.average_score = sum(scores) / len(scores)
        analysis.set_top_locations(dict(locations.most_common(5)))
        analysis.set_top_sources(dict(sources.most_common(5)))
        analysis.set_day_histogram(dict(days))

        charts = {
            "sentiment": self._chart_sentiment(sentiments),
            "sources": self._chart_sources(sources),
            "locations": self._chart_locations(locations),
            "timeline": self._chart_timeline(days),
        }
        analysis.set_charts(charts)

        session.add(analysis)
        session.commit()
        session.refresh(analysis)
        return analysis

    def _encode_plot(self):
        buffer = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buffer, format="png")
        plt.close()
        buffer.seek(0)
        return base64.b64encode(buffer.read()).decode("utf-8")

    def _chart_sentiment(self, sentiments: Counter) -> str:
        labels = list(sentiments.keys()) or ["No Data"]
        sizes = list(sentiments.values()) or [1]
        colors = ["#22c55e", "#f97316", "#2563eb"]
        plt.figure(figsize=(4, 4))
        plt.pie(sizes, labels=labels, autopct="%1.1f%%", colors=colors[: len(labels)])
        plt.title("Sentiment Mix")
        return self._encode_plot()

    def _chart_sources(self, sources: Counter) -> str:
        labels = list(sources.keys())
        values = list(sources.values())
        plt.figure(figsize=(5, 4))
        plt.bar(labels, values, color="#2563eb")
        plt.title("Source Breakdown")
        plt.ylabel("Mentions")
        plt.xticks(rotation=30, ha="right")
        return self._encode_plot()

    def _chart_locations(self, locations: Counter) -> str:
        labels = list(locations.keys())
        values = list(locations.values())
        plt.figure(figsize=(5, 4))
        plt.barh(labels, values, color="#8b5cf6")
        plt.xlabel("Mentions")
        plt.title("Top Regions")
        return self._encode_plot()

    def _chart_timeline(self, days: Counter) -> str:
        sorted_days = sorted(days.items(), key=lambda x: x[0])
        labels = [datetime.strptime(d, "%Y-%m-%d") for d, _ in sorted_days]
        values = [v for _, v in sorted_days]
        plt.figure(figsize=(6, 3))
        plt.plot(labels, values, marker="o", color="#f97316")
        plt.title("Mentions Over Time")
        plt.ylabel("Volume")
        plt.gcf().autofmt_xdate()
        return self._encode_plot()
