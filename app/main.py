import base64
import io
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict

from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from .database import Base, SessionLocal, engine, get_session
from .models import Analysis, Job, JobStatus, Post
from .services.analyzer import Analyzer
from .services.collector import Collector
from .services.notifier import Notifier

app = FastAPI(title="Sentiment & Demographic Research")
templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

executor = ThreadPoolExecutor(max_workers=2)
collector = Collector()
analyzer = Analyzer()
notifier = Notifier()


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


def process_job(job_id: int):
    session = SessionLocal()
    try:
        job = session.get(Job, job_id)
        if not job:
            return
        job.status = JobStatus.processing
        session.commit()

        posts = collector.collect(session, job)
        analysis = analyzer.analyze(session, job, posts)

        job.status = JobStatus.completed
        job.completed_at = datetime.utcnow()
        job.message = None
        session.commit()
        notifier.notify("user", f"Analysis ready for '{job.query}'.")
    except Exception as exc:  # pragma: no cover
        job = session.get(Job, job_id)
        if job:
            job.status = JobStatus.failed
            job.message = str(exc)
            session.commit()
    finally:
        session.close()


@app.get("/", response_class=HTMLResponse)
def home(request: Request, db: Session = Depends(get_session)):
    recent_jobs = db.query(Job).order_by(Job.created_at.desc()).limit(5).all()
    return templates.TemplateResponse(
        "index.html", {"request": request, "jobs": recent_jobs, "default_limit": 50000}
    )


@app.post("/jobs")
def create_job(
    request: Request,
    query: str = Form(...),
    limit: int = Form(50000),
):
    db = SessionLocal()
    try:
        job = Job(query=query.strip(), limit=limit or 50000)
        db.add(job)
        db.commit()
        db.refresh(job)
    finally:
        db.close()
    executor.submit(process_job, job.id)
    return RedirectResponse(url=f"/jobs/{job.id}", status_code=303)


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_detail(request: Request, job_id: int, db: Session = Depends(get_session)):
    job = db.get(Job, job_id)
    if not job:
        return HTMLResponse(content="Job not found", status_code=404)

    analysis = job.analysis
    posts_preview = (
        db.query(Post)
        .filter(Post.job_id == job_id)
        .order_by(Post.collected_at.desc())
        .limit(10)
        .all()
    )
    return templates.TemplateResponse(
        "job.html",
        {
            "request": request,
            "job": job,
            "analysis": analysis,
            "posts_preview": posts_preview,
        },
    )


@app.get("/jobs/{job_id}/export/csv")
def export_csv(job_id: int, db: Session = Depends(get_session)):
    posts = db.query(Post).filter(Post.job_id == job_id).order_by(Post.collected_at).all()
    if not posts:
        return HTMLResponse(content="No posts found for export", status_code=404)

    def iter_rows():
        yield "source,location,sentiment,score,content\n"
        for p in posts:
            content = p.content.replace("\n", " ").replace(",", " ")
            yield f"{p.source},{p.author_location},{p.sentiment_label},{p.sentiment_score:.2f},{content}\n"

    headers = {"Content-Disposition": f"attachment; filename=job-{job_id}-data.csv"}
    return StreamingResponse(iter_rows(), media_type="text/csv", headers=headers)


@app.get("/jobs/{job_id}/export/pdf")
def export_pdf(job_id: int, db: Session = Depends(get_session)):
    analysis = db.query(Analysis).filter(Analysis.job_id == job_id).first()
    job = db.get(Job, job_id)
    if not job or not analysis:
        return HTMLResponse(content="Analysis not ready", status_code=404)

    from fpdf import FPDF

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, "Insight Report", ln=1)

    pdf.set_font("Arial", size=12)
    pdf.cell(0, 10, f"Query: {job.query}", ln=1)
    pdf.cell(0, 10, f"Records analyzed: {analysis.total_count}", ln=1)
    pdf.cell(
        0,
        10,
        f"Sentiment => Positive: {analysis.positive_count}, Neutral: {analysis.neutral_count}, Negative: {analysis.negative_count}",
        ln=1,
    )
    pdf.cell(0, 10, f"Average score: {analysis.average_score:.2f}", ln=1)

    charts = analysis.charts_dict()
    for key in ["sentiment", "timeline"]:
        chart_data = charts.get(key)
        if not chart_data:
            continue
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            tmp.write(base64.b64decode(chart_data))
            tmp.flush()
            pdf.image(tmp.name, w=180)
        os.unlink(tmp.name)

    pdf_output = pdf.output(dest="S")
    output_bytes = pdf_output.encode("latin1") if isinstance(pdf_output, str) else bytes(pdf_output)
    headers = {"Content-Disposition": f"attachment; filename=job-{job_id}-report.pdf"}
    return StreamingResponse(io.BytesIO(output_bytes), media_type="application/pdf", headers=headers)


@app.get("/jobs/{job_id}/export/chart/{chart_name}")
def export_chart(job_id: int, chart_name: str, db: Session = Depends(get_session)):
    analysis = db.query(Analysis).filter(Analysis.job_id == job_id).first()
    if not analysis:
        return HTMLResponse(content="Analysis not ready", status_code=404)
    charts: Dict[str, str] = analysis.charts_dict()
    data = charts.get(chart_name)
    if not data:
        return HTMLResponse(content="Chart not found", status_code=404)
    img_bytes = base64.b64decode(data)
    headers = {"Content-Disposition": f"attachment; filename={chart_name}-job-{job_id}.png"}
    return StreamingResponse(io.BytesIO(img_bytes), media_type="image/png", headers=headers)


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
