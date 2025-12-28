import os

from datetime import datetime

from fastapi.testclient import TestClient

from app import main
from app.database import Base, SessionLocal, engine
from app.models import Analysis, Job, JobStatus, Post


def setup_function():
    # reset database for isolation
    if os.path.exists("app.db"):
        engine.dispose()
        os.remove("app.db")
    Base.metadata.create_all(bind=engine)


def test_process_job_creates_analysis():
    original_collector = main.collector
    try:
        main.collector = make_stub_collector()
        session = SessionLocal()
        job = Job(query="battery tech", limit=30)
        session.add(job)
        session.commit()
        session.refresh(job)
        session.close()

        main.process_job(job.id)

        check = SessionLocal()
        refreshed = check.get(Job, job.id)
        assert refreshed.status == JobStatus.completed
        analysis = check.query(Analysis).filter(Analysis.job_id == job.id).first()
        assert analysis is not None
        assert analysis.total_count == 30
        assert analysis.positive_count + analysis.neutral_count + analysis.negative_count == 30
        check.close()
    finally:
        main.collector = original_collector


def test_export_endpoints():
    original_collector = main.collector
    try:
        main.collector = make_stub_collector()
        session = SessionLocal()
        job = Job(query="ai", limit=20)
        session.add(job)
        session.commit()
        session.refresh(job)
        session.close()

        main.process_job(job.id)

        client = TestClient(main.app)
        csv_response = client.get(f"/jobs/{job.id}/export/csv")
        assert csv_response.status_code == 200
        assert csv_response.headers["content-type"].startswith("text/csv")
        assert "source,location,sentiment" in csv_response.text

        pdf_response = client.get(f"/jobs/{job.id}/export/pdf")
        assert pdf_response.status_code == 200
        assert pdf_response.headers["content-type"].startswith("application/pdf")

        chart_response = client.get(f"/jobs/{job.id}/export/chart/sentiment")
        assert chart_response.status_code == 200
        assert chart_response.headers["content-type"].startswith("image/png")
    finally:
        main.collector = original_collector


def make_stub_collector():
    class StubCollector:
        def collect(self, session, job):
            posts = []
            for i in range(job.limit):
                label = ["positive", "neutral", "negative"][i % 3]
                post = Post(
                    job_id=job.id,
                    source="Stub",
                    author_location="Earth",
                    content=f"Sample content {i}",
                    collected_at=datetime.utcnow(),
                    sentiment_label=label,
                    sentiment_score=0.1 if label == "positive" else -0.1 if label == "negative" else 0.0,
                )
                session.add(post)
                posts.append(post)
            session.commit()
            return posts

    return StubCollector()
