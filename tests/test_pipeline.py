import os

from fastapi.testclient import TestClient

from app import main
from app.database import Base, SessionLocal, engine
from app.models import Analysis, Job, JobStatus


def setup_function():
    # reset database for isolation
    if os.path.exists("app.db"):
        os.remove("app.db")
    Base.metadata.create_all(bind=engine)


def test_process_job_creates_analysis():
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


def test_export_endpoints():
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
