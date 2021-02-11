from fastapi import FastAPI, HTTPException
from db_connection import CreateEngine

from sqlalchemy import func
from sqlalchemy import select
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
        # select authors.author_name, authors.author_email from authors
        # join (SELECT distinct(mapping_id) as id FROM authors) as mapped
        # where mapped.id = authors.id;
        select_mapping = select(func.distinct(authors_table.c.mapping_id).label('id')).alias('mapped')
        select_authors = select([authors_table.c.author_name, authors_table.c.author_email]
                            ).select_from(select_mapping.join(authors_table, authors_table.c.id == select_mapping.c.id))
        authors = connection.execute(select_authors)

        return [ {'name' : a.author_name, 'email' : a.author_email} for a in authors]

@gitstats_app.get("/unmapped_authors/")
def unmapped_authors():
    with engine.connect() as connection:

        select_unmapped = authors_table.select().where(authors_table.c.mapping_id == 0)
        authors = connection.execute(select_unmapped)

        return [ {'name' : a.author_name, 'email' : a.author_email, 'id' : a.id} for a in authors]

@gitstats_app.put("/unmapped_authors/")
def create_item(request_data: MapUserRequest):
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
