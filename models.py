from sqlalchemy import (
    create_engine, Column, Integer, String, Text,
    Boolean, Date, DateTime, JSON
)
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import os

Base = declarative_base()


class Listing(Base):
    __tablename__ = "listings"

    id          = Column(Integer, primary_key=True)
    source      = Column(String(50), nullable=False)       # greenhouse, tspa, osint-jobs
    company     = Column(String(200), nullable=False)
    title       = Column(String(300), nullable=False)
    location    = Column(String(200))
    url         = Column(String(500), unique=True, nullable=False)
    date_posted = Column(Date)
    raw_jd      = Column(Text)
    salary_min  = Column(Integer)
    salary_max  = Column(Integer)
    is_agency   = Column(Boolean, default=False)

    # LLM scoring output
    scored      = Column(Boolean, default=False)
    best_track  = Column(String(100))   # Threat Intel | Fraud/T&S | Detection Eng | Incident Ops
    fit_score   = Column(Integer)       # 1-10
    gaps        = Column(JSON)          # list of strings describing gaps
    jd_phrases  = Column(JSON)          # list of JD phrases not covered by resume

    # User actions
    dismissed   = Column(Boolean, default=False)
    applied     = Column(Boolean, default=False)

    created_at  = Column(DateTime, default=datetime.utcnow)


def get_engine():
    url = os.environ["DATABASE_URL"]
    # Render gives postgres:// but SQLAlchemy 2.x needs postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return create_engine(url)


def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def init_db():
    engine = get_engine()
    Base.metadata.create_all(engine)
