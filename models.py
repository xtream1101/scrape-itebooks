from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import ProgrammingError, IntegrityError
from scrapers import raw_config

Base = declarative_base()

SCHEMA = 'it-ebooks'
# Used when schema cannot be used
table_prefix = ''

if not raw_config.get('database', 'uri').startswith('postgres'):
    SCHEMA = None
    table_prefix = SCHEMA + '_'


class Book(Base):
    __tablename__ = table_prefix + 'book'
    __table_args__ = {'schema': SCHEMA}
    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(Integer, unique=True)
    file_location = Column(String(300))
    file_cover_location = Column(String(300))
    file_cover_source = Column(String(200))
    description = Column(Text)
    file_source = Column(String(200))
    format = Column(String(10))
    isbn = Column(String(20))
    language = Column(String(20))
    pages = Column(Integer)
    publisher = Column(String(100))
    title = Column(String(512))
    year = Column(Integer)
    author = Column(String(200))


class Setting(Base):
    __tablename__ = table_prefix + 'setting'
    __table_args__ = {'schema': SCHEMA}
    id = Column(Integer, primary_key=True)
    book_last_ran = Column(DateTime)
    book_last_id = Column(Integer)
    bit = Column(Integer, unique=True)


engine = create_engine(raw_config.get('database', 'uri'))

if raw_config.get('database', 'uri').startswith('postgres'):
    try:
        engine.execute(CreateSchema(SCHEMA))
    except ProgrammingError:
        # Schema already exists
        pass

Base.metadata.create_all(engine)

Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)

db_session = DBSession()

try:
    new_setting = Setting()
    new_setting.bit = 0
    db_session.add(new_setting)
    db_session.commit()
except IntegrityError:
    # Settings row has already been created
    db_session.rollback()
