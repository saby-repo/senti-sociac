# Social Insight Lab

> Real-time sentiment intelligence across Reddit, Hacker News and Indian news feeds — no API keys required.

Built with **FastAPI · SQLite · Matplotlib · Jinja2**. Launch a query, get back sentiment breakdowns, keyword trends, a geographic source map, and export-ready reports in seconds.

---

## Features

| | |
|---|---|
| 📡 **5 Live Sources** | Reddit (topic-routed across 14 categories), Hacker News (Algolia), Times of India, NDTV, The Hindu |
| 🧠 **Sentiment Analysis** | Positive / Neutral / Negative per post with compound scoring and negation handling |
| 📊 **7 Charts** | Sentiment pie · Source breakdown · Sentiment by source · Volume timeline · Top keywords · Score distribution · Top authors |
| 🗺 **Source Map** | Leaflet.js geographic map — pin size = post volume, color = dominant sentiment |
| 💾 **Exports** | CSV (all posts + scores) · PDF summary report · Individual chart PNGs |
| ⚡ **No API keys** | Reddit via public JSON, HN via free Algolia API, RSS direct XML parsing |

---

## Quick Start

```bash
# 1. Clone and set up
git clone https://github.com/saby-repo/senti-sociac.git
cd senti-sociac
python -m venv .venv

# 2. Activate (Windows)
.venv\Scripts\activate
# Activate (Mac/Linux)
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Start the server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Open **http://localhost:8000**, type a query (e.g. `EV charging`, `ChatGPT`, `iran missile`), and hit **Launch Analysis**.

### Environment (optional)

Copy `.env.example` to `.env`. The only variable you may want to set is:

```
DATABASE_URL=sqlite:///./app.db   # default — no change needed
```

No Twitter/Reddit/NewsAPI credentials are required.

---

## How It Works

```
Query → Collector (ThreadPoolExecutor, 8 workers)
          ├── RedditClient     — 50% budget, topic-routed subreddits, cursor pagination
          ├── HackerNewsClient — 25% budget, Algolia search API
          └── NewsRSSClient    — 25% budget, Times of India · NDTV · The Hindu
       → Deduplication by title[:120]
       → SimpleSentimentAnalyzer (VADER-style compound scoring)
       → Matplotlib charts (thread-safe, base64 encoded)
       → SQLite (Jobs · Posts · Analyses)
       → Jinja2 dashboard
```

Budget split, concurrency, and source routing are all configurable inside `app/services/collector.py`.

---

## Project Layout

```
senti-sociac/
├── app/
│   ├── main.py              # FastAPI routes, process_job(), export endpoints
│   ├── models.py            # Job, Post, Analysis (SQLAlchemy)
│   ├── database.py          # Engine, SessionLocal, migrations
│   ├── services/
│   │   ├── collector.py     # SubredditRouter, RedditClient, HackerNewsClient, NewsRSSClient
│   │   ├── analyzer.py      # Sentiment scoring, 7 Matplotlib charts, geo mapping
│   │   └── notifier.py      # Stub notifier (stdout; wire to email/Slack/webhook)
│   ├── templates/
│   │   ├── index.html       # Landing: hero search, feature grid, jobs table
│   │   └── job.html         # Job detail: stat cards, sentiment bar, map, charts, exports
│   └── static/
│       └── styles.css       # Premium light theme (Inter, indigo/cyan accents)
├── tests/
│   ├── conftest.py          # In-memory SQLite fixtures
│   ├── test_collector.py    # 24 tests — router, RSS, HN, integration, sentiment
│   └── test_pipeline.py     # 5 tests — process_job, exports, health, validation
├── .env.example
└── requirements.txt
```

---

## API & Export Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Dashboard — recent jobs |
| `POST` | `/jobs` | Launch a new analysis job |
| `GET` | `/jobs/{id}` | Job detail with charts and map |
| `POST` | `/jobs/{id}/retry` | Re-run an existing job |
| `POST` | `/jobs/{id}/delete` | Delete a job and its data |
| `GET` | `/jobs/{id}/export/csv` | Download all posts as CSV |
| `GET` | `/jobs/{id}/export/pdf` | Download PDF summary report |
| `GET` | `/jobs/{id}/export/chart/{name}` | Download a chart PNG (`sentiment`, `sources`, `sentiment_by_source`, `timeline`, `word_freq`, `score_dist`, `locations`) |
| `GET` | `/health` | Health check |

---

## Running Tests

```bash
# All 29 tests (no server needed, uses in-memory SQLite)
.venv/Scripts/python -m pytest tests/ -v
```

---

## Extending

- **Add a source** — implement a client with a `collect(query, limit) -> list[dict]` interface and register it in `Collector`.
- **Swap the sentiment model** — replace `SimpleSentimentAnalyzer` in `analyzer.py` with any model that returns a compound score in `[-1, 1]`.
- **Add notifications** — wire `Notifier` in `notifier.py` to email, Slack, or webhooks.
- **Scale out** — swap `ThreadPoolExecutor` for Celery/RQ and SQLite for Postgres for production volume.
