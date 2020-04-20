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
        Column('author_email', String(length=100), unique=True))

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
    
    
    # load commits
    print('Uploading commits')
    upload_counter=0

    with open(commits_data_file, 'r') as input_file:
        loaded_commits = json.load(input_file)

        insert_connection = engine.connect()

        for c in loaded_commits:
            if not 'hash' in c:
                continue

            if c['hash'] not in existing_hashes:
                author_email = c['author_email']
                author_id = -1
                if author_email in authors:
                    author_id = authors[author_email]
                else:
                    with engine.connect() as connection:
                        ins_stmt = all_authors_table.insert().values(author_email=author_email)
                        ins_result = connection.execute(ins_stmt)
                        author_id = ins_result.inserted_primary_key
                        authors[author_email] = author_id
                
                if c['insertions'] == 0 and c['deletions'] == 0:
                    print('Warnning: empty commit {} from {}'.format(c['hash'], author_email))

                upload_counter = upload_counter + 1

                commit_dt = datetime.fromisoformat(c['date'])
                insert_stmt = repo_commits_table.insert().values(commit_hash= c['hash'],
                    author_id=author_id,
                    is_merge=False,
                    commit_date_time=commit_dt,
                    commit_date=commit_dt.date(),
                    additions=c['insertions'],
                    removals=c['deletions'])
                
                insert_connection.execute(insert_stmt)

                
    print('Added {} commits'.format(upload_counter))

    # load merges
    print('Uploading merge commits')
    upload_counter=0

    with open(merge_commits_data_file, 'r') as input_file:
        loaded_commits = json.load(input_file)

        insert_connection = engine.connect()

        for c in loaded_commits:
            if not 'hash' in c:
                continue

            if c['hash'] not in existing_hashes:
                author_email = c['author_email']
                author_id = -1
                if author_email in authors:
                    author_id = authors[author_email]
                else:
                    with engine.connect() as connection:
                        ins_stmt = all_authors_table.insert().values(author_email=author_email)
                        ins_result = connection.execute(ins_stmt)
                        author_id = ins_result.inserted_primary_key
                
                upload_counter = upload_counter + 1

                commit_dt = datetime.fromisoformat(c['date'])
                insert_stmt = repo_commits_table.insert().values(commit_hash= c['hash'],
                    author_id=author_id,
                    is_merge=True,
                    commit_date_time=commit_dt,
                    commit_date=commit_dt.date(),
                    additions=0,
                    removals=0)
                
                insert_connection.execute(insert_stmt)
    print('Added {} merge commits'.format(upload_counter))