from db_connection import CreateEngine

from sqlalchemy import Boolean
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import func
from sqlalchemy import MetaData, Table, Column, Integer, Text, String,ForeignKey
from sqlalchemy import select, update

import argparse
import datetime
import json
import requests
import subprocess
import sys

def load_pr_stats(user, token):
    engine = CreateEngine()
    metadata = MetaData()

    authors_table = Table('authors', metadata, autoload=True, autoload_with=engine)
    all_repo_table = Table('repo_id', metadata, autoload=True, autoload_with=engine)

    github_authors_table = Table('github_users', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('author_id', Integer, ForeignKey('authors.id')),
        Column('github_user_name', String(length=100), nullable=False, unique=True)
    )

    pr_info_table = Table('github_pr_info', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('repo_id', Integer, ForeignKey('repo_id.id')),
        Column('pr_number', Integer, nullable=False),
        Column('github_user_id', Integer, ForeignKey('github_users.id')),
        Column('submit_date', Date, nullable=False)
    )

    metadata.create_all(engine)

    repo_to_process = []

    sel_repo_statement = all_repo_table.select()
    with engine.connect() as connection:
        result = connection.execute(sel_repo_statement)
        for repo_def in result:
            if repo_def.host == 'github.com':
                path = repo_def.path
                repo_id = repo_def.id

                if path.endswith('.git'):
                    path = path[:-4]

                repo_to_process.append((repo_id, path))

    prs_per_page = 50

    github_users = {}
    # load all existing github authors
    with engine.connect() as connection:
        select_users_statemnt = select([ github_authors_table.c.id.label('id')
                                       , github_authors_table.c.github_user_name.label('name')])
        for r in connection.execute(select_users_statemnt):
            github_users[r.name] = r.id

    for r in repo_to_process:
        print(f'{r[0]} : {r[1]}')
        # load existing PRs for that repo
        select_prs_statement = select([pr_info_table.c.pr_number.label('number')]).where(pr_info_table.c.repo_id==r[0])
        with engine.connect() as connection:
            loaded_prs = [n.number for n in connection.execute(select_prs_statement)]

        # load prs until all done
        continue_loading = True
        current_page = 0
        added_prs = 0
        request_url = f'https://api.github.com/repos/{r[1]}/pulls'
        while continue_loading:
            query_params = {'state' : 'all',
                            'page' : current_page,
                            'per_page' : prs_per_page}

            request_result = requests.get(request_url, params=query_params, auth=(user, token))
            if request_result.status_code != 200:
                print(f'Failed to load on {r[1]} : {request_result.status_code}')
                break

            response_data = request_result.json()

            if len(response_data) == 0:
                break

            print(f'loaded {len(response_data)} PRs on page {current_page}')

            for pr_info in response_data:
                pr_id = pr_info['number']
                if pr_id not in loaded_prs:
                    author_name = pr_info['user']['login']
                    created_at = datetime.datetime.strptime(pr_info['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                    author_id = None

                    # check that author exists
                    if author_name in github_users:
                        author_id = github_users[author_name]
                    else:
                        # register new github user
                        with engine.connect() as connection:
                            ins_stmt = github_authors_table.insert().values(author_id=1, github_user_name=author_name)
                            ins_result = connection.execute(ins_stmt)
                            author_id = ins_result.inserted_primary_key[0]
                            github_users[author_name] = author_id
                            print(f'Added GH user: {author_name}')

                    # add pr info
                    with engine.connect() as connection:
                        ins_stmt = pr_info_table.insert().values(repo_id=r[0], pr_number=pr_id, github_user_id=author_id, submit_date=created_at)
                        ins_result = connection.execute(ins_stmt)
                        added_prs = added_prs + 1
                        loaded_prs.append(pr_id)
                else:
                    continue_loading = False

            current_page = current_page + 1

        print(f'{added_prs} added to {r[1]}')

    return 0

if __name__ == '__main__':
    input_parser = argparse.ArgumentParser()
    input_parser.add_argument('--user',
                                action='store',
                                type=str,
                                required=True,
                                help='User name')

    input_parser.add_argument('--token',
                                action='store',
                                type=str,
                                required=True,
                                help='Github token')

    args = input_parser.parse_args()

    exit(load_pr_stats(user=args.user, token=args.token))
