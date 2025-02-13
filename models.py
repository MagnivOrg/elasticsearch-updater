from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Float,
    Boolean,
    ForeignKey,
    JSON,
    Index,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import relationship
from settings import DB_CONFIG
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()


class AnalyticsData(Base):
    __tablename__ = "analytics_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    request_log_id = Column(Integer, ForeignKey("request_logs.id", ondelete="CASCADE"), nullable=True)
    workspace_id = Column(Integer, ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=True)
    prompt_id = Column(Integer, ForeignKey("prompt_registry.id", ondelete="SET NULL"), nullable=True)
    prompt_name = Column(String, nullable=True)

    request_start_time = Column(DateTime, nullable=True)
    request_end_time = Column(DateTime, nullable=True)
    price = Column(Float, nullable=True)
    tokens = Column(Integer, nullable=True)
    engine = Column(String, nullable=True)
    tags = Column(JSON, default=[])
    analytics_metadata = Column(JSON, default={})
    synced = Column(Boolean, nullable=False, default=False, index=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now(), index=True)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Relationships
    workspace = relationship("Workspace", back_populates="analytics_entries")
    request_logs = relationship("RequestLogs", back_populates="analytics_entries")

    __table_args__ = (
        UniqueConstraint("workspace_id", "request_start_time", name="_workspace_time_uc"),
        Index("idx_workspace_id", "workspace_id"),
        Index("idx_synced", "synced"),
        Index("idx_created_at", "created_at"),
    )

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


def init_db():
    """Initialize the database tables."""
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    init_db()
    print("✅ Database tables created successfully!")