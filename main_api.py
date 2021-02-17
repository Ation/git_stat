from fastapi import FastAPI, HTTPException
from db_connection import CreateEngine

from sqlalchemy import func
from sqlalchemy import select, update
from sqlalchemy import MetaData, Table

from pydantic import BaseModel


class MapUserRequest(BaseModel):
    user_id : int
    mapping_id : int

gitstats_app = FastAPI()

engine = CreateEngine()
metadata = MetaData()
authors_table = Table('authors', metadata, autoload=True, autoload_with=engine)
all_repo_table = Table('repo_id', metadata, autoload=True, autoload_with=engine)

@gitstats_app.get("/")
async def root():
    return {"message": "Hello World"}

@gitstats_app.get("/test")
async def test_path():
    return {"message": "Test path"}

@gitstats_app.get("/authors/")
def authors():
    with engine.connect() as connection:
        select_authors = authors_table.select()
        authors = connection.execute(select_authors)

        result = {}

        for entry in authors:
            author_id = entry.id
            mapping_id = entry.mapping_id
            email = entry.author_email
            name = entry.author_name

            if mapping_id not in result:
                record = {}
                record['mapped_emails'] = []
                record['mapped_names'] = []
                result[mapping_id] = record
            else:
                record = result[mapping_id]

            if mapping_id == author_id:
                record['name'] = name
                record['email'] = email
            else:
                record['mapped_names'].append(name)
                record['mapped_emails'].append(email)

        return result

@gitstats_app.get("/authors/self_mapped/")
def self_mapped_():
    with engine.connect() as connection:

        select_self_mapped = authors_table.select().where(authors_table.c.mapping_id == authors_table.c.id)
        authors = connection.execute(select_self_mapped)

        return [ {'name' : a.author_name, 'email' : a.author_email, 'id' : a.id} for a in authors]

@gitstats_app.get("/authors/unmapped/")
def unmapped_authors():
    with engine.connect() as connection:

        select_unmapped = authors_table.select().where(authors_table.c.mapping_id == 0)
        authors = connection.execute(select_unmapped)

        return [ {'name' : a.author_name, 'email' : a.author_email, 'id' : a.id} for a in authors]

@gitstats_app.put("/authors/map_author")
def map_author(request_data: MapUserRequest):
    # map request_data.user_id to request_data.mapping_id
    with engine.connect() as connection:
        update_stmt = update(authors_table).where(authors_table.c.id == request_data.user_id).values(mapping_id=request_data.mapping_id)
        connection.execute(update_stmt)
    return {'result' : 'ok'}

def get_repo_collection():
    with engine.connect() as connection:
        sel_repo_statement = all_repo_table.select()
        with engine.connect() as connection:
            result = connection.execute(sel_repo_statement)
            return { r.name : r.id  for r in result }

@gitstats_app.get("/repo/")
def repo_collection():
    return [ { 'id' : v, 'name' : k}  for k,v in get_repo_collection().items()]

@gitstats_app.get("/repo/{repo_name}")
async def get_repo_by_name(repo_name : str):
    all_repo = get_repo_collection()
    if repo_name not in all_repo:
        raise HTTPException(status_code=404, detail=f'Repo {repo_name} not found')

    id = all_repo[repo_name]
    return {'id' : id}
