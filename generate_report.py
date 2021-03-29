# select authors.author_name, stats.commit_count, stats.additions, stats.removals
# from (select
#     total.author_id as author_id
#     , COUNT(total.commit_hash) as commit_count
#     , SUM(total.additions) as additions
#     , SUM(total.removals) as removals
#  from
#     (
#       select authors.mapping_id as author_id
#           , all_commits.commit_hash as commit_hash
#           , all_commits.additions as additions
#           , all_commits.removals as removals
#       from all_commits
#       join authors on authors.id = all_commits.author_id
#       where all_commits.is_merge = 0) as total
#     group by total.author_id) as stats
#         join authors on stats.author_id = authors.id;

# ToDo
# filter authors with total commits count lesser than threshold
# width for columns to fit author name

from datetime import datetime, date
from sqlalchemy import and_
from sqlalchemy import Boolean
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import MetaData, Table, Column, Integer, Text, String,ForeignKey
from sqlalchemy import select
from sqlalchemy import text

from report_collector import ReportRange, ReportCollector, RepoInfo
from excel_generator import ExcelGenerator
from db_connection import CreateEngine

import argparse
import json
import sys
import xlsxwriter

def add_month(target):
    if target.month == 12:
        return target.replace(year=target.year+1, month=1)

    return target.replace(month=target.month+1)

if __name__ == '__main__':

    input_parser = argparse.ArgumentParser()
    input_parser.add_argument('--repo_names',
                                action='store',
                                type=str,
                                required=True,
                                help='Name of repo to process ( in DB ). Or list of names comma separated')

    input_parser.add_argument('--from_date',
                                action='store',
                                type=str,
                                required=False,
                                help='Starting from date ( MM-YYYY ) ')

    input_parser.add_argument('--till_date',
                                action='store',
                                type=str,
                                required=False,
                                help='Ending date date ( MM-YYYY ) ')

    input_parser.add_argument('--min_commits',
                                action='store',
                                type=int,
                                default=0,
                                help='Minimal number of commits for all period')

    input_parser.add_argument('--generator',
                            action='store',
                            type=str,
                            required=False,
                            default='excel',
                            help='Report generator tool [excel|bokeh]')

    args = input_parser.parse_args()

    print('Repos: {}'.format(args.repo_names))

    if args.from_date is not None:
        parts=args.from_date.split('-')
        if len(parts) != 2:
            print('ERROR: from date format MM-YYYY')
            exit(1)

        from_month = int(parts[0])
        from_year = int(parts[1])

        if from_month <= 0 or from_month > 12:
            print('ERROR: wrong month')
            exit(1)

        start_from = date(year=from_year, month=from_month, day=1)
    else:
        start_from = None

    if args.till_date is not None:
        parts=args.till_date.split('-')
        if len(parts) != 2:
            print('ERROR: till date format MM-YYYY')
            exit(1)

        from_month = int(parts[0])
        from_year = int(parts[1])

        if from_month <= 0 or from_month > 12:
            print('ERROR: wrong month for till_dat')
            exit(1)

        till_date = date(year=from_year, month=from_month, day=1)
    else:
        till_date = None

    # connect to db
    db_user_name = 'gitstat'
    db_password = 'gitstat'
    db_host = 'localhost'
    db_name = 'gitstat'

    engine = CreateEngine()

    metadata = MetaData()

    all_repo_table = Table('repo_id', metadata, autoload=True, autoload_with=engine)
    repo_table = Table('all_commits', metadata, autoload=True, autoload_with=engine)
    authors_table = Table('authors', metadata, autoload=True, autoload_with=engine)

    sel_repo_statement = all_repo_table.select()
    repo_map = {}

    with engine.connect() as connection:
        result = connection.execute(sel_repo_statement)
        repo_map = {}
        for x in result:
            repo_map[x.name] = x

    repo_ids = []
    repo_list = []

    if args.repo_names == '*':
        repo_ids = [x.id for x in repo_map.values()]
        repo_list = [RepoInfo(x.name, f'https://{x.host}/{x.path}') for x in repo_map.values()]
    else:
        repo_names = args.repo_names.split(',')
        for name in repo_names:
            if name not in repo_map:
                print(f'ERROR: {name} is undefined repo name')
                exit(1)

            x = repo_map[name]
            repo_ids.append(x.id)
            repo_list.append(RepoInfo(x.name, f'https://{x.host}/{x.path}'))

    # get min and max date
    select_dates_stmt = select([
        func.min(repo_table.c.commit_date).label('min_date'),
        func.max(repo_table.c.commit_date).label('max_date')
    ]).where(repo_table.c.repo_id.in_(repo_ids))

    with engine.connect() as connection:
        result = connection.execute(select_dates_stmt).first()

    min_date = result['min_date']
    max_date = result['max_date']

    min_date = min_date.replace(day=1)
    max_date = add_month(max_date.replace(day=1))

    if start_from is not None and min_date < start_from:
        min_date = start_from

    if till_date is not None and max_date > till_date:
        max_date = till_date

    from_date = min_date
    to_date = add_month(min_date)

    commit_limit = args.min_commits
    print('Min commits : {}'.format(commit_limit))
    print('Start from : {}'.format(min_date))
    print('Till date  : {}'.format(to_date))

    with engine.connect() as connection:
        # get all authors and all commits for a full period
        all_commits_select = select(
            [
                authors_table.c.mapping_id.label('author_id'),
                repo_table.c.commit_hash.label('commit_hash')
            ]).select_from(repo_table.join(authors_table, authors_table.c.id == repo_table.c.author_id)
            ).where(
                and_(repo_table.c.commit_date >= min_date
                     , repo_table.c.commit_date < max_date
                     , repo_table.c.repo_id.in_(repo_ids))
            ).alias('all_commits__')

        aggregate_stmt = select([all_commits_select.c.author_id.label('author_id'),
                                func.count(func.distinct(all_commits_select.c.commit_hash)).label('commit_count')
                        ]).select_from(all_commits_select
                        ).group_by(all_commits_select.c.author_id
                        ).alias('aggregated_commits__')

        mapped_total_stmt = select([authors_table.c.author_name.label('author_name'),
                                   aggregate_stmt.c.commit_count.label('commit_count')
                                   ]).select_from(aggregate_stmt.join(authors_table, authors_table.c.id == aggregate_stmt.c.author_id)
                                ).where(aggregate_stmt.c.commit_count >= commit_limit
                                ).alias('mapped_total__')

        total = connection.execute(mapped_total_stmt)

        all_authors_in_repo = set()

        print('Cotribution summary:')
        for r in total:
            print('  {} : {}'.format(r.author_name, r.commit_count))
            all_authors_in_repo.add(r.author_name)

        authors_list = [ x for x in all_authors_in_repo]

        if len(authors_list) == 0:
            print('There are no contributors')
            exit(0)

        report = ReportCollector(author_list=authors_list, repo_list=repo_list)

        while from_date != max_date:
            # do the query
            total_commits_stmt = select(
                [
                    authors_table.c.mapping_id.label('author_id'),
                    repo_table.c.commit_hash.label('commit_hash'),
                    repo_table.c.additions.label('additions'),
                    repo_table.c.removals.label('removals'),
                    repo_table.c.commit_date.label('commit_date'),
                    repo_table.c.is_merge.label('is_merge')
                ]).select_from(repo_table.join(authors_table, authors_table.c.id == repo_table.c.author_id)
                ).where(
                    and_(
                        repo_table.c.commit_date >= from_date
                        , repo_table.c.commit_date < to_date
                        , repo_table.c.repo_id.in_(repo_ids)
                    )
                ).alias()

            stats_commits_stmt = select(
                [
                    total_commits_stmt.c.author_id.label('author_id'),
                    func.count(func.distinct(total_commits_stmt.c.commit_hash)).label('commit_count'),
                    func.sum(total_commits_stmt.c.additions).label('additions'),
                    func.sum(total_commits_stmt.c.removals).label('removals'),
                    func.count(func.distinct(total_commits_stmt.c.commit_date)).label('days_with_commit')
                ]
            ).select_from(
                total_commits_stmt
            ).where(
                total_commits_stmt.c.is_merge == False
            ).group_by(
                total_commits_stmt.c.author_id
            ).alias()

            stats_merges_stmt = select(
                [
                    total_commits_stmt.c.author_id.label('author_id'),
                    func.count(func.distinct(total_commits_stmt.c.commit_hash)).label('commit_count')
                ]
            ).select_from(
                total_commits_stmt
            ).where(
                total_commits_stmt.c.is_merge == True
            ).group_by(
                total_commits_stmt.c.author_id
            ).alias()

            report_commits_statement = select(
                [
                    authors_table.c.author_name.label('author_name'),
                    stats_commits_stmt.c.commit_count.label('commit_count'),
                    stats_commits_stmt.c.additions.label('additions'),
                    stats_commits_stmt.c.removals.label('removals'),
                    stats_commits_stmt.c.days_with_commit.label('days_with_commit')
                ]
            ).select_from(
                stats_commits_stmt.join(authors_table, authors_table.c.id == stats_commits_stmt.c.author_id)
            )

            report_merges_statement = select(
                [
                    authors_table.c.author_name.label('author_name'),
                    stats_merges_stmt.c.commit_count.label('commit_count')
                ]
            ).select_from(
                stats_merges_stmt.join(authors_table, authors_table.c.id == stats_merges_stmt.c.author_id)
            )

            report_commits = connection.execute(report_commits_statement)
            report_merges = connection.execute(report_merges_statement)

            # set data to report
            print('Processing range: {} - {}'.format(from_date, to_date))
            reportRange = ReportRange(from_date, to_date)

            for r in report_commits:
                current_report = report.getReportEntry(reportRange, r['author_name'])
                if current_report is not None:
                    current_report.commits = r['commit_count']
                    current_report.additions = r['additions']
                    current_report.removals = r['removals']

            for r in report_merges:
                current_report = report.getReportEntry(reportRange, r['author_name'])
                if current_report is not None:
                    current_report.merges = r['commit_count']

            from_date = to_date
            to_date = add_month(to_date)

    outputFileName='report.xlsx'
    print(f'Generating report to {outputFileName}...')
    generator = ExcelGenerator(outputFileName=outputFileName)
    generator.GenerateReport(report)
