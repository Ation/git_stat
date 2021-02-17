from fastapi import FastAPI, HTTPException
from db_connection import CreateEngine

from sqlalchemy import func
from sqlalchemy import select, update
from sqlalchemy import MetaData, Table
from sqlalchemy import desc

from pydantic import BaseModel


class MapUserRequest(BaseModel):
    user_id : int
    mapping_id : int

gitstats_app = FastAPI()

engine = CreateEngine()
metadata = MetaData()
authors_table = Table('authors', metadata, autoload=True, autoload_with=engine)
all_repo_table = Table('repo_id', metadata, autoload=True, autoload_with=engine)
repo_table = Table('all_commits', metadata, autoload=True, autoload_with=engine)

@gitstats_app.get("/")
async def root():
    return {"message": "Hello World"}

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
def repo_contributors(repo_name : str):
    all_repo = get_repo_collection()
    if repo_name not in all_repo:
        raise HTTPException(status_code=404, detail=f'Repo {repo_name} not found')

    id = all_repo[repo_name]

    all_commits_select = select(
        [
            authors_table.c.mapping_id.label('author_id'),
            repo_table.c.commit_hash.label('commit_hash')
        ]).select_from(repo_table.join(authors_table, authors_table.c.id == repo_table.c.author_id)
        ).where(repo_table.c.repo_id == id
        ).alias('all_commits__')

    aggregate_stmt = select([all_commits_select.c.author_id.label('author_id'),
                            func.count(func.distinct(all_commits_select.c.commit_hash)).label('commit_count')
                    ]).select_from(all_commits_select
                    ).group_by(all_commits_select.c.author_id
                    ).alias('aggregated_commits__')

    mapped_total_stmt = select([authors_table.c.author_name.label('author_name'),
                               aggregate_stmt.c.commit_count.label('commit_count')
                               ]).select_from(aggregate_stmt.join(authors_table, authors_table.c.id == aggregate_stmt.c.author_id)
                            ).order_by(desc(aggregate_stmt.c.commit_count)
                            ).alias('mapped_total__')

    with engine.connect() as connection:
        total = connection.execute(mapped_total_stmt)
        return [ {r.author_name : r.commit_count} for r in total]
