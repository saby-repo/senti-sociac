import enum
import json
from datetime import datetime
from typing import Dict, Optional

from sqlalchemy import Column, DateTime, Enum, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from .database import Base


class JobStatus(str, enum.Enum):
    pending = "pending"
    processing = "processing"
    completed = "completed"
    failed = "failed"


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    query = Column(String, nullable=False)
    limit = Column(Integer, default=50000)
    status = Column(Enum(JobStatus), default=JobStatus.pending, nullable=False)
    message = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

    analysis = relationship("Analysis", uselist=False, back_populates="job")
    posts = relationship("Post", back_populates="job", cascade="all, delete-orphan")


class Post(Base):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    source = Column(String, nullable=False)
    author_location = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    collected_at = Column(DateTime, default=datetime.utcnow)
    sentiment_label = Column(String, nullable=False)
    sentiment_score = Column(Float, nullable=False)

    job = relationship("Job", back_populates="posts")


class Analysis(Base):
    __tablename__ = "analyses"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, unique=True)
    total_count = Column(Integer, default=0)
    positive_count = Column(Integer, default=0)
    negative_count = Column(Integer, default=0)
    neutral_count = Column(Integer, default=0)
    average_score = Column(Float, default=0)
    top_locations = Column(Text, default="{}")
    top_sources = Column(Text, default="{}")
    day_histogram = Column(Text, default="{}")
    charts = Column(Text, default="{}")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    job = relationship("Job", back_populates="analysis")

    def top_locations_dict(self) -> Dict[str, int]:
        return json.loads(self.top_locations or "{}")

    def top_sources_dict(self) -> Dict[str, int]:
        return json.loads(self.top_sources or "{}")

    def day_histogram_dict(self) -> Dict[str, int]:
        return json.loads(self.day_histogram or "{}")

    def charts_dict(self) -> Dict[str, str]:
        return json.loads(self.charts or "{}")

    def set_top_locations(self, data: Dict[str, int]):
        self.top_locations = json.dumps(data)

    def set_top_sources(self, data: Dict[str, int]):
        self.top_sources = json.dumps(data)

    def set_day_histogram(self, data: Dict[str, int]):
        self.day_histogram = json.dumps(data)

    def set_charts(self, data: Dict[str, str]):
        self.charts = json.dumps(data)
