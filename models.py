from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, DateTime,
    ForeignKey
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


class Weekday(Base):
    __tablename__ = 'weekday'
    __table_args__ = {'schema': 'sql_scheduler'}

    job_day    = Column(Text, primary_key=True)    # Ex: 'Monday'
    day_number = Column(Integer, nullable=False)   # Ex: 1

    # Um dia pode ter vários agendamentos
    jobs = relationship(
        'JobDE',
        back_populates='weekday',
        cascade='all, delete-orphan'
    )


class JobHE(Base):
    __tablename__ = 'jobs_he'
    __table_args__ = {'schema': 'sql_scheduler'}

    job_id      = Column(Integer, primary_key=True)
    job_name    = Column(Text, nullable=False)
    job_status  = Column(String(1), nullable=False, default='N')  # 'Y' ou 'N'
    export_path = Column(Text, nullable=False)
    export_name = Column(Text, nullable=False)
    sql_script  = Column(Text)
    last_exec   = Column(DateTime)

    # Relação de agendamentos
    schedule = relationship(
        'JobDE',
        back_populates='job',
        cascade='all, delete-orphan'
    )


class JobDE(Base):
    __tablename__ = 'jobs_de'
    __table_args__ = {'schema': 'sql_scheduler'}

    schedule_id = Column(Integer, primary_key=True)
    job_id      = Column(
        Integer,
        ForeignKey('sql_scheduler.jobs_he.job_id', ondelete='CASCADE'),
        nullable=False
    )
    job_day     = Column(
        Text,
        ForeignKey('sql_scheduler.weekday.job_day', ondelete='CASCADE'),
        nullable=False
    )
    start_hour  = Column(Text, nullable=False)
    end_hour    = Column(Text)
    job_iter    = Column(Text)

    # Relações reversas
    job     = relationship('JobHE',    back_populates='schedule')
    weekday = relationship('Weekday', back_populates='jobs')


class Log(Base):
    __tablename__ = 'logs'
    __table_args__ = {'schema': 'sql_scheduler'}

    log_id      = Column(Integer, primary_key=True)
    timestamp   = Column(DateTime, default=datetime.now, nullable=False)
    log_level   = Column(Text)
    logger_name = Column(Text)
    job_id      = Column(Integer)
    user_name   = Column(Text)
    log_text    = Column(Text, nullable=False)
    duration_ms = Column(Integer)
