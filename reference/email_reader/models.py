# models.py
from sqlalchemy import (
    Column, Integer, String, Date, Text, DateTime,
    PrimaryKeyConstraint
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy import insert # You can still use the generic insert, but make sure the dialect is loaded
import datetime
import os
import dotenv
from dotenv import load_dotenv
load_dotenv()

CUSTOMER_CAR_TABLE = os.getenv("CUSTOMER_CAR_TABLE")
DATABASE_SCHEMA = os.getenv("DATABASE_SCHEMA")
AUDIT_TABLE= os.getenv("AUDIT_TABLE")
#metadata = MetaData(schema="kylemore_schema")
#Base = declarative_base(metadata=metadata)
#Base = declarative_base()

# Set default schema for all models (SQLAlchemy 2.0+)
#Base.metadata.schema = DATABASE_SCHEMA   # ← this is enough!
Base = declarative_base()
Base.metadata.schema = DATABASE_SCHEMA   # ← this is enough!
#engine = create_engine(DATABASE_URL, echo=False)
#SessionLocal = sessionmaker(bind=engine)
#Base.metadata.create_all(engine)
os.makedirs("logs", exist_ok=True)

class CustomerCarData(Base):
    __tablename__ = CUSTOMER_CAR_TABLE
    __schema__ = DATABASE_SCHEMA

    customer_number = Column(Integer, nullable=False)
    vehicle_number = Column(Integer, nullable=False)

    customer_full_name = Column(String(100), nullable=False)
    telephone_number = Column(String(20), nullable=False)
    email_address = Column(String(100), nullable=False)
    enquiry_creation_date = Column(Date, nullable=True)
    enquiry_number = Column(Integer, nullable=False)
    deposit_date = Column(Date, nullable=True)
    invoice_date = Column(Date, nullable=True)
    model = Column(String(50), nullable=True)
    variant = Column(String(50), nullable=True)
    colour_code = Column(String(50), nullable=True)
    registration_number = Column(String(50), nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint("customer_number", "vehicle_number"),
    )


class IngestionAudit(Base):
    __tablename__ = AUDIT_TABLE
    __schema__ = DATABASE_SCHEMA

    id = Column(Integer, primary_key=True, autoincrement=True)
    email_id = Column(String(200))
    received_at = Column(DateTime, default=datetime.datetime.utcnow)
    processed_at = Column(DateTime, default=datetime.datetime.utcnow)
    raw_body = Column(Text)
    rows_extracted = Column(Integer)
    rows_inserted = Column(Integer)
    status = Column(String(200))
    error = Column(String(200))

