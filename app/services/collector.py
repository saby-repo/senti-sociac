"""
collector.py — Multi-source data collector.

Sources (all free, no API key required)
────────────────────────────────────────
1. Reddit     — public JSON API  (reddit.com/r/{sub}/search.json)
2. Hacker News— Algolia search   (hn.algolia.com/api/v1/search)
3. Indian news— direct RSS XML   (TOI · NDTV · The Hindu)
"""

import logging
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple

import requests
from sqlalchemy.orm import Session

from ..models import Job, Post

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
_USER_AGENT   = "SocialInsightLab/1.0 (open-source research tool)"
_MAX_PER_PAGE = 100
_MAX_RETRIES  = 3
_REDDIT_BASE  = "https://www.reddit.com"
_HN_SEARCH    = "https://hn.algolia.com/api/v1/search"

_NEWS_RSS_FEEDS: List[Tuple[str, str]] = [
    ("Times of India", "https://timesofindia.indiatimes.com/rssfeedstopstories.cms"),
    ("NDTV",           "https://feeds.feedburner.com/ndtvnews-top-stories"),
    ("The Hindu",      "https://www.thehindu.com/feeder/default.rss"),
]


# ── Topic catalogue ────────────────────────────────────────────────────────────
_TOPIC_CATALOGUE: List[Tuple[str, List[str], List[str]]] = [
    ("news",
     ["news", "breaking", "headline", "report", "journalist", "media",
      "press", "bulletin", "alert", "update"],
     ["news", "worldnews", "UpliftingNews", "nottheonion"]),
    ("politics",
     ["politic", "government", "election", "vote", "democrat", "republican",
      "parliament", "senate", "congress", "president", "policy", "law",
      "legislation", "geopolit",
      # military / conflict / international relations
      "war", "military", "missile", "attack", "conflict", "sanction",
      "nuclear", "bomb", "weapon", "troop", "army", "navy", "strike",
      "invasion", "coup", "protest", "riot", "terror", "nato", "un ",
      "iran", "russia", "ukraine", "china", "israel", "hamas", "hezbollah",
      "taliban", "pakistan", "india", "diplomacy", "treaty", "ceasefire",
      "refugee", "civilian", "drone", "airstrike", "artillery"],
     ["worldnews", "geopolitics", "politics", "worldpolitics",
      "PoliticalDiscussion", "war"]),
    ("technology",
     ["tech", "software", "hardware", "developer", "programming", "code",
      "coding", "computer", "internet", "app", "startup", "silicon",
      "gadget", "device", "chip", "semiconductor", "robot"],
     ["technology", "programming", "hardware", "softwaregore", "techsupport"]),
    ("artificial_intelligence",
     ["ai", "artificial intelligence", "machine learning", "deep learning",
      "neural", "llm", "gpt", "chatgpt", "openai", "anthropic", "claude",
      "gemini", "llama", "generative", "diffusion", "transformer"],
     ["artificial", "MachineLearning", "ChatGPT", "LocalLLaMA", "singularity"]),
    ("science",
     ["science", "research", "study", "physics", "chemistry", "biology",
      "astronomy", "space", "nasa", "rocket", "planet", "evolution",
      "dna", "gene", "experiment", "lab", "discovery"],
     ["science", "askscience", "space", "Physics", "biology", "chemistry"]),
    ("finance",
     ["stock", "invest", "market", "economy", "economic", "finance",
      "financial", "fund", "bond", "equity", "dividend", "ipo",
      "nasdaq", "dow", "fed", "interest rate", "inflation", "recession",
      "gdp", "trade", "tariff", "forex", "hedge"],
     ["investing", "stocks", "finance", "economics", "personalfinance",
      "wallstreetbets"]),
    ("cryptocurrency",
     ["crypto", "bitcoin", "btc", "ethereum", "eth", "blockchain", "nft",
      "defi", "altcoin", "coin", "token", "wallet", "mining", "solana",
      "doge", "dogecoin", "binance", "exchange"],
     ["CryptoCurrency", "Bitcoin", "ethereum", "CryptoMarkets", "dogecoin"]),
    ("sports",
     ["sport", "football", "soccer", "basketball", "nba", "nfl", "baseball",
      "mlb", "tennis", "golf", "cricket", "rugby", "hockey", "nhl",
      "formula", "f1", "racing", "athlete", "team", "championship",
      "tournament", "match", "game", "league"],
     ["sports", "nfl", "nba", "soccer", "baseball", "tennis", "formula1"]),
    ("gaming",
     ["game", "gaming", "playstation", "xbox", "nintendo", "pc gaming",
      "steam", "esport", "fps", "rpg", "mmo", "minecraft", "fortnite",
      "valorant", "lol", "league of legends", "gamer", "console"],
     ["gaming", "Games", "pcgaming", "PS5", "xboxone", "nintendo"]),
    ("entertainment",
     ["movie", "film", "cinema", "tv", "television", "series", "show",
      "netflix", "disney", "streaming", "music", "song", "album", "artist",
      "celebrity", "actor", "actress", "award", "oscar", "grammy"],
     ["movies", "television", "Music", "entertainment"]),
    ("health",
     ["health", "medical", "medicine", "doctor", "hospital", "disease",
      "virus", "vaccine", "mental health", "fitness", "diet", "nutrition",
      "wellness", "symptom", "therapy", "drug", "pharma", "covid",
      "pandemic", "cancer", "diabetes"],
     ["health", "medicine", "Fitness", "nutrition", "mentalhealth", "AskDocs"]),
    ("environment",
     ["environment", "climate", "global warming", "carbon", "renewable",
      "solar", "wind energy", "fossil fuel", "pollution", "sustainability",
      "recycle", "biodiversity", "extinction", "deforestation", "ocean",
      "green", "eco"],
     ["environment", "climate", "sustainability", "ZeroWaste",
      "ClimateActionPlan"]),
    ("business",
     ["business", "company", "corporate", "ceo", "entrepreneur", "startup",
      "brand", "product", "revenue", "profit", "earnings", "merger",
      "acquisition", "ipo", "saas", "marketing", "ecommerce"],
     ["business", "entrepreneur", "startups", "smallbusiness", "marketing"]),
    ("education",
     ["education", "school", "university", "college", "student", "learning",
      "teach", "degree", "course", "curriculum", "study", "exam",
      "scholarship", "academic"],
     ["education", "college", "Teachers", "learnprogramming", "AskAcademia"]),
]

_DEFAULT_SUBREDDITS = ["all"]


# ── SubredditRouter ────────────────────────────────────────────────────────────
class SubredditRouter:
    def route(self, query: str) -> Tuple[str, List[str]]:
        q = query.lower()
        scores: Dict[str, int] = {}
        for cat, keywords, _ in _TOPIC_CATALOGUE:
            for kw in keywords:
                if kw in q:
                    scores[cat] = scores.get(cat, 0) + 1
        if not scores:
            logger.info("No category matched for %r — general search.", query)
            return "general", _DEFAULT_SUBREDDITS
        best = max(scores, key=lambda c: scores[c])
        _, _, subs = next(r for r in _TOPIC_CATALOGUE if r[0] == best)
        logger.info("Query %r → category=%r subs=%s", query, best, subs)
        return best, subs


# ── Shared RawPost ─────────────────────────────────────────────────────────────
@dataclass
class RawPost:
    title: str
    body: str
    author: str
    source: str
    created_utc: float
    url: str = ""
    score: int = 0
    num_comments: int = 0


# ── SimpleSentimentAnalyzer ────────────────────────────────────────────────────
class SimpleSentimentAnalyzer:
    _POSITIVE = {
        "good", "great", "excellent", "love", "amazing", "excited", "celebrate",
        "growth", "happy", "fantastic", "best", "wonderful", "brilliant",
        "outstanding", "superb", "positive", "win", "success", "improve",
        "gain", "rise", "strong", "optimistic", "hope",
    }
    _NEGATIVE = {
        "bad", "terrible", "awful", "hate", "concern", "worried", "decline",
        "risk", "worst", "horrible", "poor", "failure", "disaster", "crisis",
        "negative", "loss", "broken", "wrong", "fall", "crash", "fear",
        "dangerous", "corrupt", "fraud",
    }
    _NEGATION = {"not", "no", "never", "neither", "nor", "without"}

    def evaluate(
        self,
        text: str,
        existing_label: Optional[str] = None,
        existing_score: Optional[float] = None,
    ) -> Tuple[str, float]:
        if existing_label and existing_score is not None:
            return existing_label, existing_score
        tokens = re.findall(r"[a-zA-Z']+", text.lower())
        score, negate = 0, False
        for token in tokens:
            if token in self._NEGATION:
                negate = True
                continue
            ws = 1 if token in self._POSITIVE else (-1 if token in self._NEGATIVE else 0)
            score += -ws if negate else ws
            negate = False
        n = score / max(len(tokens), 1)
        label = "positive" if n > 0.05 else "negative" if n < -0.05 else "neutral"
        return label, n


# ── RedditClient ───────────────────────────────────────────────────────────────
class RedditClient:
    def __init__(self, user_agent: str = _USER_AGENT, cooldown: float = 1.0):
        self.cooldown = cooldown
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})

    @property
    def configured(self) -> bool:
        return True

    def fetch_subreddit(
        self, subreddit: str, query: str, limit: int, sort: str = "new"
    ) -> List[RawPost]:
        if subreddit == "all":
            url = f"{_REDDIT_BASE}/search.json"
            params: Dict = {"q": query, "sort": sort, "t": "all"}
        else:
            url = f"{_REDDIT_BASE}/r/{subreddit}/search.json"
            params = {"q": query, "restrict_sr": "1", "sort": sort, "t": "all"}
        return self._paginate(url, params, limit, subreddit)

    def _paginate(self, url: str, params: Dict, limit: int, label: str) -> List[RawPost]:
        collected: List[RawPost] = []
        after: Optional[str] = None
        retries = 0
        while len(collected) < limit:
            p = {**params, "limit": min(_MAX_PER_PAGE, limit - len(collected))}
            if after:
                p["after"] = after
            try:
                resp = self._session.get(url, params=p, timeout=10)
            except requests.RequestException as exc:
                logger.warning("Reddit error (%s): %s", label, exc)
                break
            if resp.status_code == 429:
                retries += 1
                if retries > _MAX_RETRIES:
                    break
                time.sleep(self.cooldown * (2 ** retries))
                continue
            if not resp.ok:
                logger.warning("Reddit %d for %s", resp.status_code, label)
                break
            retries = 0
            data = resp.json().get("data", {})
            for child in data.get("children", []):
                d = child.get("data", {})
                sub = d.get("subreddit", label)
                hn_url = d.get("url", "")
                # For Reddit, use the post's permalink as fallback
                permalink = d.get("permalink", "")
                post_url = hn_url if hn_url and not hn_url.startswith("https://www.reddit.com") \
                    else (f"https://www.reddit.com{permalink}" if permalink else hn_url)
                collected.append(RawPost(
                    title=d.get("title", ""),
                    body=d.get("selftext", "") or "",
                    author=d.get("author", "[deleted]"),
                    source=f"Reddit/r/{sub}",
                    created_utc=float(d.get("created_utc", 0)),
                    url=post_url,
                    score=int(d.get("score", 0)),
                    num_comments=int(d.get("num_comments", 0)),
                ))
                if len(collected) >= limit:
                    break
            after = data.get("after")
            if not after:
                break
            time.sleep(self.cooldown)
        return collected


# ── HackerNewsClient ───────────────────────────────────────────────────────────
class HackerNewsClient:
    """
    Uses the Algolia HN Search API — free, no auth required.
    https://hn.algolia.com/api/v1/search?query=...&tags=story
    """

    def __init__(self, cooldown: float = 0.5):
        self.cooldown = cooldown
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": _USER_AGENT})

    @property
    def configured(self) -> bool:
        return True

    def fetch(self, query: str, limit: int) -> List[RawPost]:
        collected: List[RawPost] = []
        page = 0
        retries = 0

        while len(collected) < limit:
            params = {
                "query": query,
                "tags": "story",
                "hitsPerPage": min(_MAX_PER_PAGE, limit - len(collected)),
                "page": page,
            }
            try:
                resp = self._session.get(_HN_SEARCH, params=params, timeout=10)
            except requests.RequestException as exc:
                logger.warning("HN request error: %s", exc)
                break

            if resp.status_code == 429:
                retries += 1
                if retries > _MAX_RETRIES:
                    logger.warning("HN rate-limited; stopping.")
                    break
                time.sleep(self.cooldown * (2 ** retries))
                continue

            if not resp.ok:
                logger.warning("HN API returned %d", resp.status_code)
                break

            retries = 0
            payload = resp.json()
            hits = payload.get("hits", [])
            if not hits:
                break

            for hit in hits:
                oid = hit.get("objectID", "")
                # External URL; fall back to HN discussion page
                url = hit.get("url") or f"https://news.ycombinator.com/item?id={oid}"
                body = hit.get("story_text") or ""
                body = re.sub(r"<[^>]+>", "", body).strip()  # strip HTML
                collected.append(RawPost(
                    title=hit.get("title", ""),
                    body=body,
                    author=hit.get("author", ""),
                    source="Hacker News",
                    created_utc=_parse_iso(hit.get("created_at")),
                    url=url,
                    score=int(hit.get("points") or 0),
                    num_comments=int(hit.get("num_comments") or 0),
                ))
                if len(collected) >= limit:
                    break

            if page >= payload.get("nbPages", 1) - 1:
                break
            page += 1
            time.sleep(self.cooldown)

        logger.info("Hacker News → %d stories.", len(collected))
        return collected


def _parse_iso(ts: Optional[str]) -> float:
    if not ts:
        return datetime.now(timezone.utc).timestamp()
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return datetime.now(timezone.utc).timestamp()


# ── NewsRSSClient ──────────────────────────────────────────────────────────────
class NewsRSSClient:
    """Direct XML RSS — Times of India, NDTV, The Hindu. No API key needed."""

    def __init__(
        self,
        feeds: Optional[List[Tuple[str, str]]] = None,
        user_agent: str = _USER_AGENT,
    ):
        self.feeds = feeds if feeds is not None else _NEWS_RSS_FEEDS
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})

    @property
    def configured(self) -> bool:
        return bool(self.feeds)

    def fetch_all(self, query: str, limit_per_feed: int) -> List[RawPost]:
        all_posts: List[RawPost] = []
        with ThreadPoolExecutor(max_workers=len(self.feeds)) as pool:
            futures = {
                pool.submit(self._fetch_feed, name, url, query, limit_per_feed): name
                for name, url in self.feeds
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    posts = future.result()
                    logger.info("%s → %d articles.", name, len(posts))
                    all_posts.extend(posts)
                except Exception as exc:
                    logger.warning("%s fetch failed: %s", name, exc)
        return all_posts

    def _fetch_feed(self, name: str, url: str, query: str, limit: int) -> List[RawPost]:
        try:
            resp = self._session.get(url, timeout=12)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
        except Exception as exc:
            logger.warning("RSS error for %s: %s", name, exc)
            return []

        items = root.findall(".//item")
        if not items:
            return []

        q_tokens = set(re.findall(r"[a-zA-Z0-9]+", query.lower()))
        matched, unmatched = [], []
        for item in items:
            raw = self._parse_item(item, name)
            combined = (raw.title + " " + raw.body).lower()
            bucket = matched if any(t in combined for t in q_tokens if len(t) > 2) else unmatched
            bucket.append(raw)

        result = matched if matched else unmatched
        logger.info("%s: %d/%d matched query.", name, len(matched), len(items))
        return result[:limit]

    def _parse_item(self, item: ET.Element, source_name: str) -> RawPost:
        title  = self._text(item, "title")
        desc   = _strip_html(self._text(item, "description"))
        link   = self._text(item, "link") or self._text(item, "guid")
        author = (
            self._text(item, "author")
            or self._text(item, "{http://purl.org/dc/elements/1.1/}creator")
            or source_name
        )
        return RawPost(
            title=title,
            body=desc,
            author=author,
            source=source_name,
            created_utc=_parse_rfc2822(self._text(item, "pubDate")),
            url=link,
        )

    @staticmethod
    def _text(el: ET.Element, tag: str) -> str:
        c = el.find(tag)
        return (c.text or "").strip() if c is not None else ""


# ── Helpers ────────────────────────────────────────────────────────────────────
def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()

def _parse_rfc2822(date_str: str) -> float:
    if not date_str:
        return datetime.now(timezone.utc).timestamp()
    try:
        return parsedate_to_datetime(date_str).timestamp()
    except Exception:
        return datetime.now(timezone.utc).timestamp()


# ── Collector ──────────────────────────────────────────────────────────────────
class Collector:
    """
    Fetches from Reddit + Hacker News + Indian news RSS feeds concurrently,
    merges, deduplicates, and sentiment-labels all results.

    Budget split (configurable via ratios):
      50 % Reddit · 25 % Hacker News · 25 % News RSS
    """

    _REDDIT_RATIO = 0.50
    _HN_RATIO     = 0.25
    # remainder goes to RSS

    def __init__(
        self,
        reddit_client: Optional[RedditClient] = None,
        hn_client: Optional[HackerNewsClient] = None,
        news_rss_client: Optional[NewsRSSClient] = None,
        router: Optional[SubredditRouter] = None,
        sentiment_analyzer: Optional[SimpleSentimentAnalyzer] = None,
    ):
        self.reddit_client    = reddit_client    or RedditClient()
        self.hn_client        = hn_client        or HackerNewsClient()
        self.news_rss_client  = news_rss_client  or NewsRSSClient()
        self.router           = router           or SubredditRouter()
        self.sentiment_analyzer = sentiment_analyzer or SimpleSentimentAnalyzer()

    def _to_post(self, job_id: int, raw: RawPost) -> Post:
        content = (raw.title + " " + raw.body).strip() if raw.body and raw.body not in (
            "[deleted]", "[removed]") else raw.title
        label, score = self.sentiment_analyzer.evaluate(content)
        ts = datetime.fromtimestamp(raw.created_utc, tz=timezone.utc)
        return Post(
            job_id=job_id,
            source=raw.source,
            author_location=raw.author,
            content=content,
            url=raw.url,
            collected_at=ts,
            sentiment_label=label,
            sentiment_score=score,
        )

    def collect(self, session: Session, job: Job) -> List[Post]:
        category, subreddits = self.router.route(job.query)

        reddit_limit = max(10, int(job.limit * self._REDDIT_RATIO))
        hn_limit     = max(10, int(job.limit * self._HN_RATIO))
        rss_limit    = max(10, job.limit - reddit_limit - hn_limit)
        per_sub      = max(1, reddit_limit // len(subreddits))
        per_feed     = max(1, rss_limit   // max(1, len(self.news_rss_client.feeds)))

        logger.info(
            "Job %d: category=%s  reddit=%d  hn=%d  rss=%d",
            job.id, category, reddit_limit, hn_limit, rss_limit,
        )

        all_raw: List[RawPost] = []
        source_counts: Dict[str, int] = {}

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures: Dict = {}

            # Reddit — one future per subreddit
            for sub in subreddits:
                futures[pool.submit(
                    self.reddit_client.fetch_subreddit, sub, job.query, per_sub
                )] = f"Reddit/r/{sub}"

            # Hacker News — single future
            futures[pool.submit(self.hn_client.fetch, job.query, hn_limit)] = "HackerNews"

            # News RSS — single future (internally concurrent per feed)
            futures[pool.submit(
                self.news_rss_client.fetch_all, job.query, per_feed
            )] = "NewsRSS"

            for future in as_completed(futures):
                label = futures[future]
                try:
                    results = future.result()
                    logger.info("%s → %d items.", label, len(results))
                    source_counts[label] = len(results)
                    all_raw.extend(results)
                except Exception as exc:
                    logger.warning("%s failed: %s", label, exc)
                    source_counts[label] = 0

        # Fallback: if Reddit returned nothing (blocked / rate-limited),
        # retry with the reliable worldnews + news subreddits
        reddit_total = sum(v for k, v in source_counts.items() if k.startswith("Reddit"))
        if reddit_total == 0 and subreddits != ["worldnews", "news"]:
            logger.info("Reddit returned 0 results; retrying with r/worldnews + r/news.")
            fallback_subs = ["worldnews", "news"]
            fallback_per_sub = max(1, reddit_limit // 2)
            with ThreadPoolExecutor(max_workers=2) as pool:
                fallback_futures = {
                    pool.submit(
                        self.reddit_client.fetch_subreddit, sub, job.query, fallback_per_sub
                    ): f"Reddit/r/{sub}(fallback)"
                    for sub in fallback_subs
                }
                for future in as_completed(fallback_futures):
                    label = fallback_futures[future]
                    try:
                        results = future.result()
                        logger.info("%s → %d items.", label, len(results))
                        source_counts[label] = len(results)
                        all_raw.extend(results)
                    except Exception as exc:
                        logger.warning("%s failed: %s", label, exc)

        if not all_raw:
            counts_str = ", ".join(f"{k}:{v}" for k, v in source_counts.items())
            raise ValueError(
                f"No posts collected for query '{job.query}' "
                f"(category={category}, subreddits={subreddits}). "
                f"Source results: [{counts_str}]. "
                "Check your query or try a more general term."
            )

        # Deduplicate by normalised title
        seen: set = set()
        posts: List[Post] = []
        for raw in all_raw:
            key = re.sub(r"\s+", " ", raw.title.lower().strip())[:120]
            if key in seen:
                continue
            seen.add(key)
            posts.append(self._to_post(job.id, raw))
            session.add(posts[-1])
            if len(posts) >= job.limit:
                break

        logger.info("Job %d: persisting %d unique posts.", job.id, len(posts))
        session.commit()
        return posts
