import base64
import csv
import io
import logging
import os
import tempfile
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func as sql_func, text
from sqlalchemy.orm import Session, joinedload

from .database import Base, SessionLocal, engine, get_session
from .models import Analysis, Job, JobStatus, Post
from .services.analyzer import Analyzer
from .services.collector import Collector
from .services.notifier import Notifier

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------
executor = ThreadPoolExecutor(max_workers=4)
collector = Collector()
analyzer = Analyzer()
notifier = Notifier()

_QUERY_MAX_LEN = 500


def _apply_migrations():
    """Idempotent column additions for SQLite (and PostgreSQL-compatible)."""
    migrations = [
        "ALTER TABLE posts ADD COLUMN url TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE analyses ADD COLUMN source_sentiments TEXT NOT NULL DEFAULT '{}'",
    ]
    with engine.connect() as conn:
        for sql in migrations:
            try:
                conn.execute(text(sql))
                conn.commit()
                logger.info("Migration applied: %s", sql[:60])
            except Exception:
                pass  # column already exists — safe to ignore


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    _apply_migrations()
    logger.info("Database tables ensured.")
    yield
    executor.shutdown(wait=False)
    logger.info("Executor shut down.")


app = FastAPI(title="Sentiment & Demographic Research", lifespan=lifespan)
import json as _json
templates = Jinja2Templates(directory="app/templates")
templates.env.filters["tojson"] = _json.dumps
app.mount("/static", StaticFiles(directory="app/static"), name="static")


# ---------------------------------------------------------------------------
# Background job processor
# ---------------------------------------------------------------------------
def process_job(job_id: int):
    session = SessionLocal()
    try:
        job = session.get(Job, job_id)
        if not job:
            logger.warning("process_job called for unknown job_id=%d", job_id)
            return
        job.status = JobStatus.processing
        session.commit()
        logger.info("Job %d started (query=%r, limit=%d).", job_id, job.query, job.limit)

        posts = collector.collect(session, job)
        analyzer.analyze(session, job, posts)

        job = session.get(Job, job_id)
        job.status = JobStatus.completed
        job.completed_at = datetime.now(timezone.utc)
        job.message = None
        session.commit()
        logger.info("Job %d completed with %d posts.", job_id, len(posts))
        notifier.notify("user", f"Analysis ready for '{job.query}'.")
    except Exception as exc:
        logger.exception("Job %d failed: %s", job_id, exc)
        try:
            session.rollback()
            job = session.get(Job, job_id)
            if job:
                job.status = JobStatus.failed
                job.message = str(exc)
                session.commit()
        except Exception as inner:
            logger.error("Could not persist failure status for job %d: %s", job_id, inner)
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    return JSONResponse({"status": "ok"})


@app.get("/", response_class=HTMLResponse)
def home(request: Request, db: Session = Depends(get_session)):
    recent_jobs = (
        db.query(Job)
        .options(joinedload(Job.analysis))
        .order_by(Job.created_at.desc())
        .limit(10)
        .all()
    )
    total     = db.query(sql_func.count(Job.id)).scalar() or 0
    completed = db.query(sql_func.count(Job.id)).filter(Job.status == JobStatus.completed).scalar() or 0
    active    = db.query(sql_func.count(Job.id)).filter(
        Job.status.in_([JobStatus.pending, JobStatus.processing])
    ).scalar() or 0
    failed    = db.query(sql_func.count(Job.id)).filter(Job.status == JobStatus.failed).scalar() or 0
    return templates.TemplateResponse(
        "index.html", {
            "request": request,
            "jobs": recent_jobs,
            "job_stats": {"total": total, "completed": completed, "active": active, "failed": failed},
        }
    )


@app.post("/jobs")
def create_job(
    query: str = Form(...),
    limit: int = Form(50000),
    db: Session = Depends(get_session),
):
    query = query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query must not be empty.")
    if len(query) > _QUERY_MAX_LEN:
        raise HTTPException(status_code=400, detail=f"Query too long (max {_QUERY_MAX_LEN} chars).")
    limit = max(1, min(limit or 50000, 100_000))

    job = Job(query=query, limit=limit)
    db.add(job)
    db.commit()
    db.refresh(job)
    logger.info("Created job %d for query=%r.", job.id, query)
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
    still_running = job.status in (JobStatus.pending, JobStatus.processing)
    return templates.TemplateResponse(
        "job.html",
        {
            "request": request,
            "job": job,
            "analysis": analysis,
            "posts_preview": posts_preview,
            "still_running": still_running,
        },
    )


@app.get("/jobs/{job_id}/export/csv")
def export_csv(job_id: int, db: Session = Depends(get_session)):
    posts = db.query(Post).filter(Post.job_id == job_id).order_by(Post.collected_at).all()
    if not posts:
        return HTMLResponse(content="No posts found for export", status_code=404)

    def iter_rows():
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["source", "author", "sentiment", "score", "url", "content"])
        yield output.getvalue()
        for p in posts:
            output.seek(0)
            output.truncate(0)
            writer.writerow([
                p.source,
                p.author_location,
                p.sentiment_label,
                f"{p.sentiment_score:.4f}",
                p.url,
                p.content,
            ])
            yield output.getvalue()

    headers = {"Content-Disposition": f"attachment; filename=job-{job_id}-data.csv"}
    return StreamingResponse(iter_rows(), media_type="text/csv; charset=utf-8", headers=headers)


@app.get("/jobs/{job_id}/export/pdf")
def export_pdf(job_id: int, db: Session = Depends(get_session)):
    analysis = db.query(Analysis).filter(Analysis.job_id == job_id).first()
    job = db.get(Job, job_id)
    if not job or not analysis:
        return HTMLResponse(content="Analysis not ready", status_code=404)

    from fpdf import FPDF, XPos, YPos

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, "Insight Report", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.set_font("Helvetica", size=12)
    pdf.cell(0, 10, f"Query: {job.query}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 10, f"Records analyzed: {analysis.total_count}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(
        0,
        10,
        f"Sentiment  Positive: {analysis.positive_count}  Neutral: {analysis.neutral_count}  Negative: {analysis.negative_count}",
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )
    pdf.cell(0, 10, f"Average score: {analysis.average_score:.4f}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    charts = analysis.charts_dict()
    tmp_files = []
    try:
        for key in ["sentiment", "timeline"]:
            chart_data = charts.get(key)
            if not chart_data:
                continue
            with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
                tmp.write(base64.b64decode(chart_data))
                tmp.flush()
                tmp_files.append(tmp.name)
            pdf.image(tmp_files[-1], w=180)
    finally:
        for path in tmp_files:
            try:
                os.unlink(path)
            except OSError:
                pass

    pdf_output = pdf.output()
    output_bytes = pdf_output if isinstance(pdf_output, bytes) else bytes(pdf_output)
    headers = {"Content-Disposition": f"attachment; filename=job-{job_id}-report.pdf"}
    return StreamingResponse(io.BytesIO(output_bytes), media_type="application/pdf", headers=headers)


@app.get("/jobs/{job_id}/export/chart/{chart_name}")
def export_chart(job_id: int, chart_name: str, db: Session = Depends(get_session)):
    valid_charts = {
        "sentiment", "sources", "locations", "timeline",
        "sentiment_by_source", "word_freq", "score_dist",
    }
    if chart_name not in valid_charts:
        raise HTTPException(status_code=400, detail=f"Unknown chart. Valid: {sorted(valid_charts)}")

    analysis = db.query(Analysis).filter(Analysis.job_id == job_id).first()
    if not analysis:
        return HTMLResponse(content="Analysis not ready", status_code=404)

    charts = analysis.charts_dict()
    data = charts.get(chart_name)
    if not data:
        return HTMLResponse(content="Chart not found", status_code=404)

    img_bytes = base64.b64decode(data)
    headers = {"Content-Disposition": f"attachment; filename={chart_name}-job-{job_id}.png"}
    return StreamingResponse(io.BytesIO(img_bytes), media_type="image/png", headers=headers)


@app.post("/jobs/{job_id}/retry")
def retry_job(job_id: int, db: Session = Depends(get_session)):
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    db.query(Analysis).filter(Analysis.job_id == job_id).delete()
    db.query(Post).filter(Post.job_id == job_id).delete()
    job.status = JobStatus.pending
    job.message = None
    job.completed_at = None
    db.commit()
    logger.info("Job %d queued for retry.", job_id)
    executor.submit(process_job, job_id)
    return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)


@app.post("/jobs/{job_id}/delete")
def delete_job(job_id: int, db: Session = Depends(get_session)):
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    db.query(Analysis).filter(Analysis.job_id == job_id).delete()
    db.query(Post).filter(Post.job_id == job_id).delete()
    db.delete(job)
    db.commit()
    logger.info("Job %d deleted.", job_id)
    return RedirectResponse(url="/", status_code=303)


if __name__ == "__main__":  # pragma: no cover
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
