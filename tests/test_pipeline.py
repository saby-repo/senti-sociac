from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app import main
from app.database import get_session
from app.models import Analysis, Job, JobStatus, Post
from app.services.collector import RawPost


# ── Stub collector ────────────────────────────────────────────────────────────

def make_stub_collector(count_override=None):
    class StubCollector:
        def collect(self, session, job):
            count = count_override if count_override is not None else job.limit
            posts = []
            for i in range(count):
                label = ["positive", "neutral", "negative"][i % 3]
                post = Post(
                    job_id=job.id,
                    source=f"Reddit/r/stub",
                    author_location=f"user_{i}",
                    content=f"Sample post content number {i}",
                    collected_at=datetime.now(timezone.utc),
                    sentiment_label=label,
                    sentiment_score=0.1 if label == "positive" else -0.1 if label == "negative" else 0.0,
                )
                session.add(post)
                posts.append(post)
            session.commit()
            return posts

    return StubCollector()


def _override_session(db_session):
    def _override():
        try:
            yield db_session
        finally:
            pass
    return _override


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_process_job_creates_analysis(db_session):
    original = main.collector
    try:
        main.collector = make_stub_collector()
        job = Job(query="battery tech", limit=30)
        db_session.add(job)
        db_session.commit()
        db_session.refresh(job)

        main.process_job(job.id)

        db_session.expire_all()
        refreshed = db_session.get(Job, job.id)
        assert refreshed.status == JobStatus.completed

        analysis = db_session.query(Analysis).filter(Analysis.job_id == job.id).first()
        assert analysis is not None
        assert analysis.total_count == 30
        assert analysis.positive_count + analysis.neutral_count + analysis.negative_count == 30
    finally:
        main.collector = original


def test_process_job_marks_failed_on_collector_error(db_session):
    class BrokenCollector:
        def collect(self, session, job):
            raise RuntimeError("Simulated API failure")

    original = main.collector
    try:
        main.collector = BrokenCollector()
        job = Job(query="fail test", limit=10)
        db_session.add(job)
        db_session.commit()
        db_session.refresh(job)

        main.process_job(job.id)

        db_session.expire_all()
        refreshed = db_session.get(Job, job.id)
        assert refreshed.status == JobStatus.failed
        assert "Simulated API failure" in (refreshed.message or "")
    finally:
        main.collector = original


def test_export_endpoints(db_session):
    original = main.collector
    try:
        main.collector = make_stub_collector()
        job = Job(query="ai news", limit=20)
        db_session.add(job)
        db_session.commit()
        db_session.refresh(job)

        main.process_job(job.id)

        main.app.dependency_overrides[get_session] = _override_session(db_session)
        client = TestClient(main.app)

        csv_resp = client.get(f"/jobs/{job.id}/export/csv")
        assert csv_resp.status_code == 200
        assert csv_resp.headers["content-type"].startswith("text/csv")
        assert "source,author,sentiment" in csv_resp.text

        pdf_resp = client.get(f"/jobs/{job.id}/export/pdf")
        assert pdf_resp.status_code == 200
        assert pdf_resp.headers["content-type"].startswith("application/pdf")

        chart_resp = client.get(f"/jobs/{job.id}/export/chart/sentiment")
        assert chart_resp.status_code == 200
        assert chart_resp.headers["content-type"].startswith("image/png")

        bad_chart = client.get(f"/jobs/{job.id}/export/chart/bogus")
        assert bad_chart.status_code == 400
    finally:
        main.collector = original
        main.app.dependency_overrides.clear()


def test_health_endpoint():
    client = TestClient(main.app)
    assert client.get("/health").json() == {"status": "ok"}


def test_create_job_validates_query():
    client = TestClient(main.app)
    assert client.post("/jobs", data={"query": "   ", "limit": 100},
                       follow_redirects=False).status_code == 400
    assert client.post("/jobs", data={"query": "x" * 501, "limit": 100},
                       follow_redirects=False).status_code == 400
