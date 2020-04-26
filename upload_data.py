import sys

from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column, Integer, Text, String,ForeignKey
from sqlalchemy import Boolean
from sqlalchemy import DateTime
from sqlalchemy import Date
from sqlalchemy import func
from sqlalchemy import select

from datetime import datetime
import json

def add_commit(target_table, connection, existing_hashes, is_merge, author_id, commit_data, repo_id=None):
    if c['hash'] in existing_hashes:
        return False

    commit_dt = datetime.fromisoformat(commit_data['date'])

    values = {}

    values['commit_hash'] = c['hash']
    values['author_id'] = author_id
    values['commit_date_time'] = commit_dt
    values['commit_date'] = commit_dt.date()

    if repo_id is not None:
        values['repo_id'] = repo_id

    if is_merge:
        values['is_merge'] = True
        values['additions'] =0
        values['removals'] =0
    else:
        values['is_merge'] = False
        values['additions'] = c['insertions']
        values['removals'] = c['deletions']

    connection.execute(target_table.insert().values(values))
    existing_hashes.add(c['hash'])

    return True

if __name__ == '__main__':
    if len(sys.argv) != 4:
        exit(1)


    repo_name = sys.argv[1]
    commits_data_file = sys.argv[2]
    merge_commits_data_file = sys.argv[3]

    # connect to db
    db_user_name = 'gitstat'
    db_password = 'gitstat'
    db_host = 'localhost'
    db_name = 'gitstat'

    engine = create_engine('mysql+mysqldb://{}:{}@{}/{}'.format(db_user_name, db_password, db_host, db_name), echo=False)

    metadata = MetaData()

    all_repo_table = Table('repo_id', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(length=40), nullable=False)
    )

    all_authors_table = Table('authors', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('author_email', String(length=100), unique=True),
        Column('author_name', String(length=100), nullable=False),
        Column('mapping_id', Integer, nullable=False, default=0))

    global_commits_table = Table('all_commits', metadata,
        Column('commit_hash', String(length=41), nullable=False, unique=True, primary_key=True),
        Column('author_id', Integer, ForeignKey('authors.id'), nullable=False),
        Column('is_merge', Boolean, nullable=False),
        Column('commit_date_time', DateTime, nullable=False),
        Column('commit_date', Date, nullable=False),
        Column('additions', Integer, nullable=False),
        Column('removals', Integer, nullable=False),
        Column('repo_id', Integer, ForeignKey('repo_id.id'))
    )

    repo_commits_table = Table(repo_name, metadata,
        Column('commit_hash', String(length=41), nullable=False, unique=True, primary_key=True),
        Column('author_id', Integer, ForeignKey('authors.id'), nullable=False),
        Column('is_merge', Boolean, nullable=False),
        Column('commit_date_time', DateTime, nullable=False),
        Column('commit_date', Date, nullable=False),
        Column('additions', Integer, nullable=False),
        Column('removals', Integer, nullable=False)
    )

    print('Creating tables-------')
    metadata.create_all(engine)

    print('Loading repo ID cash')
    sel_repo_statement = all_repo_table.select()
    repo_map = {}
    with engine.connect() as connection:
        result = connection.execute(sel_repo_statement)
        repo_map = {x.name : x.id for x in result }

    print('Loading existing authors cache')
    select_authors_statement = all_authors_table.select()
    authors = {}
    with engine.connect() as connection:
        result = connection.execute(select_authors_statement)
        authors = { x.author_email : x.id for x in result }

    print('Loading existing hashes')
    existing_hashes = set()
    with engine.connect() as connection:
        select_stmt = select([repo_commits_table.c.commit_hash])
        result = connection.execute(select_stmt)
        for row in result:
            existing_hashes.add(row.commit_hash)

    global_existing_hashes = set()
    with engine.connect() as connection:
        select_stmt = select([global_commits_table.c.commit_hash])
        result = connection.execute(select_stmt)
        for row in result:
            global_existing_hashes.add(row.commit_hash)

    if repo_name in repo_map:
        repo_id = repo_map[repo_name]
    else:
        with engine.connect() as connection:
            ins_stmt = all_repo_table.insert().values(name=repo_name)
            ins_result = connection.execute(ins_stmt)
            repo_id = ins_result.inserted_primary_key
            repo_map[repo_name]=repo_id

    # load commits
    print('Uploading commits')
    upload_counter=0
    global_upload_counter=0

    with open(commits_data_file, 'r') as input_file:
        loaded_commits = json.load(input_file)

        insert_connection = engine.connect()

        for c in loaded_commits:
            if not 'hash' in c:
                continue

            author_email = c['author_email']
            author_id = -1
            if author_email in authors:
                author_id = authors[author_email]
            else:
                with engine.connect() as connection:
                    ins_stmt = all_authors_table.insert().values(author_email=author_email, author_name=c['author_name'])
                    ins_result = connection.execute(ins_stmt)
                    author_id = ins_result.inserted_primary_key
                    authors[author_email]=author_id

            if add_commit(target_table=repo_commits_table, connection=insert_connection, existing_hashes=existing_hashes, is_merge=False, author_id=author_id, commit_data=c):
                upload_counter = upload_counter + 1

            if add_commit(target_table=global_commits_table, connection=insert_connection, existing_hashes=global_existing_hashes, is_merge=False, author_id=author_id, commit_data=c, repo_id=repo_id):
                global_upload_counter = global_upload_counter + 1


    print('Added {} commits and {} to global'.format(upload_counter, global_upload_counter))

    # load merges
    print('Uploading merge commits')
    upload_counter=0
    global_upload_counter=0

    with open(merge_commits_data_file, 'r') as input_file:
        loaded_commits = json.load(input_file)

        insert_connection = engine.connect()

        for c in loaded_commits:
            if not 'hash' in c:
                continue

            author_email = c['author_email']
            author_id = -1
            if author_email in authors:
                author_id = authors[author_email]
            else:
                with engine.connect() as connection:
                    ins_stmt = all_authors_table.insert().values(author_email=author_email, author_name=c['author_name'])
                    ins_result = connection.execute(ins_stmt)
                    author_id = ins_result.inserted_primary_key
                    authors[author_email]=author_id

            if add_commit(target_table=repo_commits_table, connection=insert_connection, existing_hashes=existing_hashes, is_merge=True, author_id=author_id, commit_data=c):
                upload_counter = upload_counter + 1

            if add_commit(target_table=global_commits_table, connection=insert_connection, existing_hashes=global_existing_hashes, is_merge=True, author_id=author_id, commit_data=c,repo_id=repo_id):
                global_upload_counter = global_upload_counter + 1

    print('Added {} merge commits and {} to global'.format(upload_counter, global_upload_counter))