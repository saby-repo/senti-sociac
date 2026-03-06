import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.database import Base
import app.database as db_module
import app.main as main_module


@pytest.fixture()
def db_session():
    """Provide an isolated in-memory SQLite session for each test.

    Patches both app.database.SessionLocal and app.main.SessionLocal so that
    all code paths (routes, process_job, get_session) use the same in-memory DB.
    """
    test_engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(bind=test_engine)
    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)

    # --- patch database module ---
    original_engine = db_module.engine
    original_session_local = db_module.SessionLocal
    db_module.engine = test_engine
    db_module.SessionLocal = TestSession

    # --- patch main module (process_job uses main.SessionLocal directly) ---
    original_main_session_local = main_module.SessionLocal
    main_module.SessionLocal = TestSession

    session = TestSession()
    try:
        yield session
    finally:
        session.close()
        # restore
        db_module.engine = original_engine
        db_module.SessionLocal = original_session_local
        main_module.SessionLocal = original_main_session_local
        Base.metadata.drop_all(bind=test_engine)
        test_engine.dispose()
