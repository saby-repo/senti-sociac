# Social Insight Lab

A FastAPI-powered sentiment and demographic research tool that simulates large-scale crawling (default 50,000 records), runs exploratory + sentiment analysis, and serves polished dashboards with exportable charts and PDF/CSV outputs.

## Features
- **Job launcher**: kick off research runs with a phrase and desired volume (defaults to 50,000 records).
- **Background processing**: simulated multi-source collection (news, social, video) with demographic tags and sentiment labels stored in SQLite.
- **Analytics**: sentiment mix, regional share, source breakdown, timeline histogram, and summary statistics.
- **Visualization + exports**: inline charts (Matplotlib), CSV data export, per-chart PNG downloads, and a branded PDF report.
- **Notifications stub**: plug-in notifier ready to connect to email/SMS/webhooks.

> Note: Crawling uses deterministic synthetic data for offline friendliness. Swap `Collector` with a real scraper to go production.

## Getting started

### Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run the server
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```
Open http://localhost:8000 to launch jobs and view dashboards.

### Data store
- SQLite file `app.db` created automatically on startup.
- Tables: `jobs`, `posts`, and `analyses` (SQLAlchemy models in `app/models.py`).

### Exports
- CSV: `/jobs/{id}/export/csv`
- PDF report: `/jobs/{id}/export/pdf`
- Individual charts (PNG): `/jobs/{id}/export/chart/{sentiment|locations|sources|timeline}`

## Testing
```bash
pytest
```

## Project layout
- `app/main.py` — FastAPI routes, background executor, export endpoints.
- `app/services/collector.py` — synthetic multi-source crawler.
- `app/services/analyzer.py` — sentiment + demographic analytics and chart rendering.
- `app/templates/` — Jinja2 templates for the dashboard.
- `app/static/styles.css` — Dark UI styling.

## Extending
- Replace `Collector.collect` with calls to real social/news APIs or scrapers.
- Wire `Notifier` to email/SMS/Slack for alerting when jobs finish.
- Scale out with a queue (Celery/RQ) and cloud database for production volume.
