"""
analyzer.py — Sentiment analytics + chart generation.

Charts produced (all stored as base64 PNG in Analysis.charts):
  sentiment          — pie chart of positive / neutral / negative mix
  sources            — bar chart of mentions by source
  locations          — horizontal bar of top regions (author names ≈ locations)
  timeline           — line chart of mentions over time
  sentiment_by_source— stacked bar: +/0/− breakdown per source
  word_freq          — top-15 keywords horizontal bar
  score_dist         — histogram of raw sentiment scores

A per-source geo enrichment is computed and stored in Analysis.source_sentiments
for the interactive Leaflet map in the UI.
"""

import base64
import io
import logging
import re
import threading
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List

import matplotlib
matplotlib.use("Agg")
import matplotlib.figure
import matplotlib.pyplot as plt
from sqlalchemy.orm import Session

from ..models import Analysis, Job, Post

logger = logging.getLogger(__name__)

_chart_lock = threading.Lock()
_TOP_N = 10

# ── Stop-words for word-frequency chart ───────────────────────────────────────
_STOPWORDS = {
    "the","a","an","and","or","but","in","on","at","to","for","of","with",
    "by","from","is","are","was","were","be","been","being","have","has","had",
    "do","does","did","will","would","could","should","may","might","can",
    "this","that","these","those","it","its","i","we","you","he","she","they",
    "what","which","who","how","when","where","why","not","no","all","as","up",
    "out","if","so","just","about","into","than","more","over","also","after",
    "new","said","says","one","two","three","get","got","make","made","like",
    "s","t","re","ve","ll","d","m",
}

# ── Source → geographic coordinates ──────────────────────────────────────────
_SOURCE_GEO: Dict[str, Dict[str, Any]] = {
    # Indian news publishers
    "Times of India": {"lat": 19.076,  "lng": 72.878,   "city": "Mumbai, India"},
    "NDTV":           {"lat": 28.614,  "lng": 77.209,   "city": "New Delhi, India"},
    "The Hindu":      {"lat": 13.083,  "lng": 80.271,   "city": "Chennai, India"},
    # Hacker News / Y Combinator HQ
    "Hacker News":    {"lat": 37.429,  "lng": -122.138, "city": "Mountain View, CA"},
}

# Subreddit name → approximate geo (community focus / Reddit HQ as fallback)
_SUBREDDIT_GEO: Dict[str, Dict[str, Any]] = {
    "news":            {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "worldnews":       {"lat": 51.507, "lng": -0.128,  "city": "London, UK"},
    "UpliftingNews":   {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "nottheonion":     {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "politics":        {"lat": 38.907, "lng": -77.037, "city": "Washington DC, USA"},
    "worldpolitics":   {"lat": 51.507, "lng": -0.128,  "city": "London, UK"},
    "geopolitics":     {"lat": 48.856, "lng":   2.352, "city": "Paris, France"},
    "technology":      {"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "programming":     {"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "hardware":        {"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "softwaregore":    {"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "techsupport":     {"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "artificial":      {"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "MachineLearning": {"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "ChatGPT":         {"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "LocalLLaMA":      {"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "singularity":     {"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "science":         {"lat": 42.360, "lng": -71.058, "city": "Boston, MA"},
    "askscience":      {"lat": 42.360, "lng": -71.058, "city": "Boston, MA"},
    "biology":         {"lat": 42.360, "lng": -71.058, "city": "Boston, MA"},
    "chemistry":       {"lat": 42.360, "lng": -71.058, "city": "Boston, MA"},
    "Physics":         {"lat": 42.360, "lng": -71.058, "city": "Boston, MA"},
    "space":           {"lat": 29.761, "lng": -95.368, "city": "Houston, TX"},
    "investing":       {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "stocks":          {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "finance":         {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "economics":       {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "personalfinance": {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "wallstreetbets":  {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "CryptoCurrency":  {"lat": 25.774, "lng": -80.194, "city": "Miami, FL"},
    "Bitcoin":         {"lat": 25.774, "lng": -80.194, "city": "Miami, FL"},
    "ethereum":        {"lat": 25.774, "lng": -80.194, "city": "Miami, FL"},
    "CryptoMarkets":   {"lat": 25.774, "lng": -80.194, "city": "Miami, FL"},
    "dogecoin":        {"lat": 25.774, "lng": -80.194, "city": "Miami, FL"},
    "sports":          {"lat": 40.813, "lng": -74.074, "city": "East Rutherford, NJ"},
    "nfl":             {"lat": 40.813, "lng": -74.074, "city": "East Rutherford, NJ"},
    "nba":             {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "soccer":          {"lat": 51.507, "lng": -0.128,  "city": "London, UK"},
    "baseball":        {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "tennis":          {"lat": 51.433, "lng": -0.214,  "city": "Wimbledon, UK"},
    "formula1":        {"lat": 43.734, "lng":   7.421, "city": "Monaco"},
    "gaming":          {"lat": 47.606, "lng": -122.332,"city": "Seattle, WA"},
    "Games":           {"lat": 47.606, "lng": -122.332,"city": "Seattle, WA"},
    "pcgaming":        {"lat": 47.606, "lng": -122.332,"city": "Seattle, WA"},
    "PS5":             {"lat": 35.689, "lng": 139.692, "city": "Tokyo, Japan"},
    "xboxone":         {"lat": 47.606, "lng": -122.332,"city": "Seattle, WA"},
    "nintendo":        {"lat": 35.689, "lng": 139.692, "city": "Tokyo, Japan"},
    "movies":          {"lat": 34.052, "lng": -118.244,"city": "Los Angeles, CA"},
    "television":      {"lat": 34.052, "lng": -118.244,"city": "Los Angeles, CA"},
    "Music":           {"lat": 36.162, "lng": -86.781, "city": "Nashville, TN"},
    "entertainment":   {"lat": 34.052, "lng": -118.244,"city": "Los Angeles, CA"},
    "health":          {"lat": 38.907, "lng": -77.037, "city": "Washington DC, USA"},
    "medicine":        {"lat": 42.360, "lng": -71.058, "city": "Boston, MA"},
    "Fitness":         {"lat": 34.052, "lng": -118.244,"city": "Los Angeles, CA"},
    "nutrition":       {"lat": 38.907, "lng": -77.037, "city": "Washington DC, USA"},
    "mentalhealth":    {"lat": 38.907, "lng": -77.037, "city": "Washington DC, USA"},
    "AskDocs":         {"lat": 38.907, "lng": -77.037, "city": "Washington DC, USA"},
    "environment":     {"lat": 47.606, "lng": -122.332,"city": "Seattle, WA"},
    "climate":         {"lat": 47.606, "lng": -122.332,"city": "Seattle, WA"},
    "sustainability":  {"lat": 47.606, "lng": -122.332,"city": "Seattle, WA"},
    "ZeroWaste":       {"lat": 47.606, "lng": -122.332,"city": "Seattle, WA"},
    "ClimateActionPlan":{"lat":47.606, "lng": -122.332,"city": "Seattle, WA"},
    "business":        {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "entrepreneur":    {"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "startups":        {"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "smallbusiness":   {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "marketing":       {"lat": 40.713, "lng": -74.006, "city": "New York, USA"},
    "education":       {"lat": 42.360, "lng": -71.058, "city": "Boston, MA"},
    "college":         {"lat": 42.360, "lng": -71.058, "city": "Boston, MA"},
    "Teachers":        {"lat": 38.907, "lng": -77.037, "city": "Washington DC, USA"},
    "learnprogramming":{"lat": 37.386, "lng": -122.084,"city": "Silicon Valley, CA"},
    "AskAcademia":     {"lat": 42.360, "lng": -71.058, "city": "Boston, MA"},
    # General fallback
    "all":             {"lat": 37.774, "lng": -122.419,"city": "San Francisco, CA"},
}
_DEFAULT_GEO = {"lat": 37.774, "lng": -122.419, "city": "San Francisco, CA"}


def _geo_for_source(source: str) -> Dict[str, Any]:
    """Return lat/lng/city for a source string like 'Reddit/r/news' or 'NDTV'."""
    if source in _SOURCE_GEO:
        return _SOURCE_GEO[source]
    if source.startswith("Reddit/r/"):
        sub = source[len("Reddit/r/"):]
        return _SUBREDDIT_GEO.get(sub, _DEFAULT_GEO)
    return _DEFAULT_GEO


# ── Analyzer ──────────────────────────────────────────────────────────────────
class Analyzer:
    def analyze(self, session: Session, job: Job, posts: List[Post]) -> Analysis:
        if not posts:
            raise ValueError("No posts to analyze")

        sentiments  = Counter(p.sentiment_label for p in posts)
        sources     = Counter(p.source           for p in posts)
        locations   = Counter(p.author_location  for p in posts)
        scores      = [p.sentiment_score for p in posts]
        days        = Counter(
            p.collected_at.strftime("%Y-%m-%d") if hasattr(p.collected_at, "strftime")
            else str(p.collected_at)[:10]
            for p in posts
        )

        # Per-source sentiment breakdown + geo
        src_sentiments: Dict[str, Any] = {}
        for post in posts:
            s = post.source
            if s not in src_sentiments:
                geo = _geo_for_source(s)
                src_sentiments[s] = {
                    "positive": 0, "neutral": 0, "negative": 0, "total": 0,
                    **geo,
                }
            src_sentiments[s][post.sentiment_label] += 1
            src_sentiments[s]["total"] += 1

        analysis = Analysis(job_id=job.id)
        analysis.total_count    = len(posts)
        analysis.positive_count = sentiments.get("positive", 0)
        analysis.negative_count = sentiments.get("negative", 0)
        analysis.neutral_count  = sentiments.get("neutral",  0)
        analysis.average_score  = sum(scores) / len(scores)
        analysis.set_top_locations(dict(locations.most_common(_TOP_N)))
        analysis.set_top_sources(dict(sources.most_common(_TOP_N)))
        analysis.set_day_histogram(dict(days))
        analysis.set_source_sentiments(src_sentiments)

        with _chart_lock:
            charts = {
                "sentiment":          self._chart_sentiment(sentiments),
                "sources":            self._chart_sources(sources),
                "locations":          self._chart_locations(locations),
                "timeline":           self._chart_timeline(days),
                "sentiment_by_source":self._chart_sentiment_by_source(src_sentiments),
                "word_freq":          self._chart_word_freq(posts),
                "score_dist":         self._chart_score_dist(scores),
            }
        analysis.set_charts(charts)
        analysis.touch()

        session.add(analysis)
        session.commit()
        session.refresh(analysis)
        logger.info("Analysis complete for job %d: %d posts.", job.id, len(posts))
        return analysis

    # ── Encoding helper ────────────────────────────────────────────────────────
    def _encode(self, fig: matplotlib.figure.Figure) -> str:
        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format="png", dpi=100, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode()

    # ── Existing charts ────────────────────────────────────────────────────────
    def _chart_sentiment(self, sentiments: Counter) -> str:
        labels = list(sentiments.keys()) or ["No Data"]
        sizes  = list(sentiments.values()) or [1]
        cmap   = {"positive": "#22c55e", "neutral": "#f97316", "negative": "#ef4444"}
        colors = [cmap.get(l, "#2563eb") for l in labels]
        fig, ax = plt.subplots(figsize=(4, 4), facecolor="#111827")
        ax.set_facecolor("#111827")
        wedges, texts, autotexts = ax.pie(
            sizes, labels=labels, autopct="%1.1f%%", colors=colors,
            textprops={"color": "#e5e7eb"}
        )
        for at in autotexts:
            at.set_color("#111827")
        ax.set_title("Sentiment Mix", color="#e5e7eb", pad=12)
        return self._encode(fig)

    def _chart_sources(self, sources: Counter) -> str:
        top    = sources.most_common(_TOP_N)
        labels = [k for k, _ in top]
        values = [v for _, v in top]
        fig, ax = plt.subplots(figsize=(6, 4), facecolor="#111827")
        ax.set_facecolor("#111827")
        bars = ax.bar(range(len(labels)), values, color="#2563eb")
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=35, ha="right", color="#9ca3af", fontsize=8)
        ax.set_ylabel("Mentions", color="#9ca3af")
        ax.tick_params(colors="#9ca3af")
        ax.spines[:].set_color("#374151")
        ax.set_title("Source Breakdown", color="#e5e7eb", pad=10)
        return self._encode(fig)

    def _chart_locations(self, locations: Counter) -> str:
        top    = locations.most_common(_TOP_N)
        labels = [k for k, _ in top]
        values = [v for _, v in top]
        fig, ax = plt.subplots(figsize=(6, 4), facecolor="#111827")
        ax.set_facecolor("#111827")
        ax.barh(labels, values, color="#8b5cf6")
        ax.set_xlabel("Mentions", color="#9ca3af")
        ax.tick_params(colors="#9ca3af")
        ax.spines[:].set_color("#374151")
        ax.set_title(f"Top {_TOP_N} Authors", color="#e5e7eb", pad=10)
        return self._encode(fig)

    def _chart_timeline(self, days: Counter) -> str:
        sorted_days = sorted(days.items())
        labels = [datetime.strptime(d, "%Y-%m-%d") for d, _ in sorted_days]
        values = [v for _, v in sorted_days]
        fig, ax = plt.subplots(figsize=(7, 3), facecolor="#111827")
        ax.set_facecolor("#111827")
        ax.plot(labels, values, marker="o", color="#f97316", linewidth=2)
        ax.fill_between(labels, values, alpha=0.15, color="#f97316")
        ax.set_ylabel("Volume", color="#9ca3af")
        ax.tick_params(colors="#9ca3af")
        ax.spines[:].set_color("#374151")
        ax.set_title("Mentions Over Time", color="#e5e7eb", pad=10)
        fig.autofmt_xdate()
        return self._encode(fig)

    # ── New charts ─────────────────────────────────────────────────────────────
    def _chart_sentiment_by_source(self, src_sentiments: Dict) -> str:
        # Only top-8 sources by total for readability
        top_sources = sorted(src_sentiments.items(),
                             key=lambda x: x[1]["total"], reverse=True)[:8]
        labels   = [s for s, _ in top_sources]
        pos_vals = [d["positive"] for _, d in top_sources]
        neu_vals = [d["neutral"]  for _, d in top_sources]
        neg_vals = [d["negative"] for _, d in top_sources]

        x = range(len(labels))
        fig, ax = plt.subplots(figsize=(7, 4), facecolor="#111827")
        ax.set_facecolor("#111827")
        ax.bar(x, pos_vals, label="Positive", color="#22c55e")
        ax.bar(x, neu_vals, bottom=pos_vals, label="Neutral",  color="#f97316")
        ax.bar(x, neg_vals,
               bottom=[p + n for p, n in zip(pos_vals, neu_vals)],
               label="Negative", color="#ef4444")
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, rotation=35, ha="right", color="#9ca3af", fontsize=8)
        ax.set_ylabel("Posts", color="#9ca3af")
        ax.tick_params(colors="#9ca3af")
        ax.spines[:].set_color("#374151")
        ax.legend(facecolor="#1f2937", labelcolor="#e5e7eb", fontsize=8)
        ax.set_title("Sentiment by Source", color="#e5e7eb", pad=10)
        return self._encode(fig)

    def _chart_word_freq(self, posts: List[Post]) -> str:
        words: Counter = Counter()
        for post in posts:
            for tok in re.findall(r"[a-zA-Z]{3,}", post.content.lower()):
                if tok not in _STOPWORDS:
                    words[tok] += 1
        top = words.most_common(15)
        if not top:
            top = [("(no data)", 1)]
        labels = [w for w, _ in reversed(top)]
        values = [c for _, c in reversed(top)]

        fig, ax = plt.subplots(figsize=(6, 5), facecolor="#111827")
        ax.set_facecolor("#111827")
        bars = ax.barh(labels, values, color="#22d3ee")
        ax.set_xlabel("Frequency", color="#9ca3af")
        ax.tick_params(colors="#9ca3af")
        ax.spines[:].set_color("#374151")
        ax.set_title("Top Keywords", color="#e5e7eb", pad=10)
        return self._encode(fig)

    def _chart_score_dist(self, scores: List[float]) -> str:
        fig, ax = plt.subplots(figsize=(6, 3), facecolor="#111827")
        ax.set_facecolor("#111827")
        n, bins, patches = ax.hist(scores, bins=20, color="#a855f7", edgecolor="#111827")
        # Colour negative bins red, positive green
        for patch, left in zip(patches, bins[:-1]):
            if left < -0.05:
                patch.set_facecolor("#ef4444")
            elif left > 0.05:
                patch.set_facecolor("#22c55e")
        ax.axvline(0, color="#9ca3af", linestyle="--", linewidth=1)
        ax.set_xlabel("Sentiment Score", color="#9ca3af")
        ax.set_ylabel("Posts", color="#9ca3af")
        ax.tick_params(colors="#9ca3af")
        ax.spines[:].set_color("#374151")
        ax.set_title("Score Distribution", color="#e5e7eb", pad=10)
        return self._encode(fig)
