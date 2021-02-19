from datetime import datetime
from urllib.parse import urlparse

import time

from db_connection import CreateEngine

from sqlalchemy import Boolean
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import func
from sqlalchemy import MetaData, Table, Column, Integer, Text, String,ForeignKey
from sqlalchemy import select, update

import json
import subprocess
import sys

# commit_hash
# author_name
# author_email
# date
# additions
# removals
# is merge

class CommitStorage():
    def __init__(self):
        self.hashes = set()
        self.commits = []

    def CollectCommits(self, commits, existing_commits):
        counter = 0
        for c in commits:
            ch = c['commit_hash']
            if ch not in self.hashes and ch not in existing_commits:
                self.commits.append(c)
                self.hashes.add(c['commit_hash'])
                counter = counter + 1
        return counter


def loadRegularCommits(dir_path, branch_name, skip_count, load_count):
    command = ['git'
            , '-C'
            , dir_path
            , 'log'
            , branch_name
            , '--no-merges'
            , '--skip'
            , str(skip_count)
            , '-n'
            , str(load_count)
            , '--pretty=format:{ "commit_hash" :"%H", "author_name" : "%aN", "author_email" : "%aE", "date" : "%aI" }', '--shortstat']
    result = subprocess.run(command, stdout=subprocess.PIPE)
    if result.returncode != 0:
        print('ERROR: Failed to get commit history')
        return None

    log_report = result.stdout.decode('utf-8').split('\n')
    if len(log_report) == 1 and log_report[0] == '':
        return []

    current_commit = None

    result = []
    for s in log_report:
        if s == '':
            result.append(current_commit)
            current_commit = None
            continue

        if s.startswith('{'):
            current_commit = json.loads(s)
            current_commit['is_merge'] = False
        else:
            stats = s.split(',')
            if len(stats) != 2 and len(stats) != 3:
                continue

            current_commit['additions']=0
            current_commit['removals']=0

            for stat in stats:
                data = stat.split(' ')

                if len(data) != 3:
                    continue

                if data[2].startswith('insertion'):
                    current_commit['additions'] = int(data[1])
                elif data[2].startswith('deletion'):
                    current_commit['removals'] = int(data[1])

    return result

def loadMergeCommits(dir_path, branch_name, skip_count, load_count):
    command = ['git'
            , '-C'
            , dir_path
            , 'log'
            , branch_name
            , '--merges'
            , '--skip'
            , str(skip_count)
            , '-n'
            , str(load_count)
            , '--pretty=format:{ "commit_hash" :"%H", "author_name" : "%aN", "author_email" : "%aE", "date" : "%aI", "is_merge" : true, "additions" : 0, "removals" : 0}']
    result = subprocess.run(command, stdout=subprocess.PIPE)
    if result.returncode != 0:
        print('ERROR: Failed to get commit history')
        return None

    log_report = result.stdout.decode('utf-8').split('\n')
    if len(log_report) == 1 and log_report[0] == '':
        return []

    return [ json.loads(s) for s in log_report if s != '']

class GitRepoInfo():
    def __init__(self, host, path, short_name = None):
        self.host = host # example github.com
        self.path = path # example user/repoName.git
        if self.path[0] == '/':
            self.path = self.path[1:]
        if short_name is None:
            self.short_name = self.GetShortName(path)

    def GetShortName(self, path):
        # path '/user/reponame.git' -> reponame
        return path.split('/')[-1].split('.')[0]

def GetRepoInfo(url):
    if url.startswith('https'):
        u = urlparse(url)
        return GitRepoInfo(u.netloc, u.path)

    u = url.split('@')[-1]
    f = u.split(':')

    return GitRepoInfo(f[0], f[1])

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Pass repo dir')
        exit(1)

    dir_path = sys.argv[1]
    command = ['git', '-C', dir_path, 'remote', '-v']
    result = subprocess.run(command, stdout=subprocess.PIPE)
    if result.returncode != 0:
        print('ERROR: Failed to get remotes info')
        exit(1)

    repo_info = None

    for remote_info in result.stdout.decode('utf-8').split('\n'):
        defs = remote_info.split('\t')
        if defs[0] == 'origin':
            parts = defs[1].split(' ')
            if (parts[1] == '(fetch)'):
                repo_info = GetRepoInfo(parts[0])
                break;

    if repo_info is None:
        print('Error: failed to get remote origin (fetch) url')
        exit(1)

    print('Loading from {} at {} {}'.format(repo_info.short_name, repo_info.host, repo_info.path))

    branches = []

    commit_storage = CommitStorage()

    # connect to DB
    engine = CreateEngine()
    metadata = MetaData()

    all_repo_table = Table('repo_id', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(length=40), nullable=False, unique=True),
        Column('host', String(length=40), nullable=False),
        Column('path', String(length=100), nullable=False)
    )

    all_authors_table = Table('authors', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('author_email', String(length=100), unique=True),
        Column('author_name', String(length=100), nullable=False),
        Column('mapping_id', Integer, nullable=False, default=0))

    global_commits_table = Table('all_commits', metadata,
        Column('id', Integer, primary_key=True),
        Column('commit_hash', String(length=41), nullable=False, unique=False),
        Column('author_id', Integer, ForeignKey('authors.id'), nullable=False),
        Column('is_merge', Boolean, nullable=False),
        Column('commit_date_time', DateTime, nullable=False),
        Column('commit_date', Date, nullable=False),
        Column('additions', Integer, nullable=False),
        Column('removals', Integer, nullable=False),
        Column('repo_id', Integer, ForeignKey('repo_id.id'))
    )

    print('Creating tables-------')
    metadata.create_all(engine)

    print('Loading existing repo info')
    repo_id = None

    sel_repo_statement = all_repo_table.select()
    with engine.connect() as connection:
        result = connection.execute(sel_repo_statement)
        for repo_def in result:
            if repo_info.short_name == repo_def.name and repo_info.path == repo_def.path:
                repo_id = repo_def.id
                break;
    if repo_id is None:
        print('Registering new repo: {}'.format(repo_info.short_name))
        with engine.connect() as connection:
            ins_stmt = all_repo_table.insert().values(name=repo_info.short_name, path=repo_info.path, host=repo_info.host)
            ins_result = connection.execute(ins_stmt)
            repo_id = ins_result.inserted_primary_key

    print('Loading existing authors cache')
    select_authors_statement = all_authors_table.select()
    authors = {}
    with engine.connect() as connection:
        result = connection.execute(select_authors_statement)
        authors = { x.author_email : x.id for x in result }

    if '' not in authors:
        print('Registering default undefined customer')
        with engine.connect() as connection:
            ins_stmt = all_authors_table.insert().values(author_email='', author_name='undefined user')
            ins_result = connection.execute(ins_stmt)
            author_id = ins_result.inserted_primary_key
            authors['']=author_id

            update_stmt = update(all_authors_table).where(all_authors_table.c.id == author_id).values(mapping_id=author_id)
            connection.execute(update_stmt)

    print('Loading existing hashes')
    global_existing_hashes = set()
    with engine.connect() as connection:
        select_stmt = select([global_commits_table.c.commit_hash]).where(global_commits_table.c.repo_id == repo_id)
        result = connection.execute(select_stmt)
        for row in result:
            global_existing_hashes.add(row.commit_hash)

    # load all remote branches list on origin
    command = ['git', '-C', dir_path, 'branch', '--all']
    result = subprocess.run(command, stdout=subprocess.PIPE)
    if result.returncode != 0:
        print('ERROR: Failed to get branch list')
        exit(1)
    for branch_string in result.stdout.decode('utf-8').split('\n'):
        branch_string = branch_string.strip()
        if '->' in branch_string:
            continue
        if branch_string.startswith('remotes/origin'):
            branches.append(branch_string)

    for branch in branches:
        print(f'Processing branch {branch}')

        total_commits_from_branch = 0
        total_merges_from_branch = 0

        skip_count_commits = 0
        skip_count_merges = 0
        load_portion = 10

        # load commits
        while True:
            commits = loadRegularCommits(dir_path, branch, skip_count_commits, load_portion)
            if len(commits) == 0:
                break

            collected_commits = commit_storage.CollectCommits(commits, global_existing_hashes)
            total_commits_from_branch = total_commits_from_branch + collected_commits

            if collected_commits != len(commits):
                break
            skip_count_commits = skip_count_commits + load_portion

        # load merges
        while True:
            merge_commits = loadMergeCommits(dir_path, branch, skip_count_merges, load_portion)

            if len(merge_commits) == 0:
                break

            collected_merges = commit_storage.CollectCommits(merge_commits, global_existing_hashes)
            total_merges_from_branch = total_merges_from_branch + collected_merges

            if collected_merges != len(merge_commits):
                break

            skip_count_merges = skip_count_merges + load_portion

        print(f'Added {total_commits_from_branch} commits and {total_merges_from_branch} merges')

    print('Uploading commits')

    start_data_upload_time = time.perf_counter()
    added_commits=0
    added_merge_commits=0
    skipped_commits=0
    skipped_merge_commits=0

    with engine.connect() as insert_connection:
        for c in commit_storage.commits:
            if not 'commit_hash' in c:
                continue

            if c['commit_hash'] in global_existing_hashes:
                if c['is_merge']:
                    skipped_merge_commits = skipped_merge_commits + 1
                else:
                    skipped_commits = skipped_commits + 1
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

                    # map to self
                    update_stmt = update(all_authors_table).where(all_authors_table.c.id == author_id).values(mapping_id=author_id)
                    connection.execute(update_stmt)


            commit_dt = datetime.fromisoformat(c['date'])

            commit_dt = datetime.fromisoformat(c['date'])
            insert_stmt = global_commits_table.insert().values(commit_hash=c['commit_hash'],
                author_id=author_id,
                is_merge=c['is_merge'],
                commit_date_time=commit_dt,
                commit_date=commit_dt.date(),
                additions=c['additions'],
                removals=c['removals'],
                repo_id=repo_id)

            insert_connection.execute(insert_stmt)

            if c['is_merge']:
                added_merge_commits = added_merge_commits + 1
            else:
                added_commits = added_commits + 1

            # no need to add this commit hash to global_existing_hashes, since it is
            # unique in this session
    end_data_upload_time = time.perf_counter()

    print(f'Added {added_commits} commits and {added_merge_commits} merge commits')
    print(f'Skipped {skipped_commits} commits and {skipped_merge_commits} merge commits')
    print(f'Uploading data time: {end_data_upload_time - start_data_upload_time} seconds')
