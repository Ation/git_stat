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

from db_connection import CreateEngine

import argparse
import json
import sys
import xlsxwriter

class Report(object):
    def __init__(self):
        self.commits = 0
        self.additions = 0
        self.removals = 0
        self.merges = 0

def get_column_name(index):
    symbols = [ chr(x) for x in range(ord('A'), ord('Z')+1) ]
    current = index % len(symbols)
    leftover = index - current
    result = symbols[current]
    if leftover == 0:
        return result
    return get_column_name(int(leftover/len(symbols))-1) + result

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

    input_parser.add_argument('--min_commits',
                                action='store',
                                type=int,
                                default=0,
                                help='Minimal number of commits for all period')


    args = input_parser.parse_args()

    print('Repos: {}'.format(args.repo_names))

    if args.from_date is not None:
        parts=args.from_date.split('-')
        if len(parts) != 2:
            print('ERROR: frim date format MM-YYYY')
            exit(1)

        from_month = int(parts[0])
        from_year = int(parts[1])

        if from_month <= 0 or from_month > 12:
            print('ERROR: wrong month')
            exit(1)

        start_from = date(year=from_year, month=from_month, day=1)
    else:
        start_from = None

    repo_names = args.repo_names.split(',')

    # connect to db
    db_user_name = 'gitstat'
    db_password = 'gitstat'
    db_host = 'localhost'
    db_name = 'gitstat'

    engine = CreateEngine()

    metadata = MetaData()
    inspector = inspect(engine)

    workbook = xlsxwriter.Workbook('{}_report.xlsx'.format('_'.join(repo_names)))
    bold = workbook.add_format({'bold': 1})

    charts_sheet = workbook.add_worksheet('charts')
    commits_sheet = workbook.add_worksheet('commits')
    additions_sheet = workbook.add_worksheet('additions')
    removals_sheet = workbook.add_worksheet('removals')
    total_commits_sheet = workbook.add_worksheet('commits_with_merges')

    all_repo_table = Table('repo_id', metadata, autoload=True, autoload_with=engine)
    repo_table = Table('all_commits', metadata, autoload=True, autoload_with=engine)
    authors_table = Table('authors', metadata, autoload=True, autoload_with=engine)

    sel_repo_statement = all_repo_table.select()
    repo_map = {}
    with engine.connect() as connection:
        result = connection.execute(sel_repo_statement)
        repo_map = {x.name : x.id for x in result }

    repo_ids = []

    for name in repo_names:
        if name not in repo_map:
            print(f'ERROR: {name} is undefined repo name')
            exit(1)

        repo_ids.append(repo_map[name])

    # get min and max date
    select_dates_stmt = select([
        func.min(repo_table.c.commit_date).label('min_date'),
        func.max(repo_table.c.commit_date).label('max_date')
    ])

    with engine.connect() as connection:
        result = connection.execute(select_dates_stmt).first()

    min_date = result['min_date']
    max_date = result['max_date']

    min_date = min_date.replace(day=1)
    max_date = add_month(max_date.replace(day=1))

    if start_from is not None and min_date < start_from:
        min_date = start_from

    from_date = min_date
    to_date = add_month(min_date)

    commit_limit = args.min_commits
    print('Min commits : {}'.format(commit_limit))

    print('Start from : {}'.format(min_date))

    with engine.connect() as connection:
        # get all authors and all commits
        all_commits_select = select(
            [
                authors_table.c.mapping_id.label('author_id'),
                repo_table.c.commit_hash.label('commit_hash')
            ]).select_from(repo_table.join(authors_table, authors_table.c.id == repo_table.c.author_id)
            ).where(
                and_(repo_table.c.commit_date >= min_date
                     , repo_table.c.repo_id.in_(repo_ids))
            ).alias()

        # count all
        counters_stmt = select(
            [
                authors_table.c.author_name.label('author_name'),
                func.count(all_commits_select.c.commit_hash).label('commit_count')
            ]).select_from(
                all_commits_select.join(authors_table, authors_table.c.id == all_commits_select.c.author_id)
            ).group_by(all_commits_select.c.author_id
            ).alias()

        total = connection.execute(counters_stmt.select().where(counters_stmt.c.commit_count > commit_limit))
        all_authors_in_repo = set()
        for r in total:
            print('{} : {}'.format(r.author_name, r.commit_count))
            all_authors_in_repo.add(r.author_name)

        authors_list = [ x for x in all_authors_in_repo]

        # write headings

        commits_sheet.write_string(0, 0, 'Month', bold)
        additions_sheet.write_string(0, 0, 'Month', bold)
        removals_sheet.write_string(0, 0, 'Month', bold)
        total_commits_sheet.write_string(0, 0, 'Month', bold)

        author_to_column = {}
        for c, a in enumerate(authors_list):
            column_index = c+1
            author_to_column[a] = column_index

            commits_sheet.write(0, column_index, a, bold)
            additions_sheet.write(0, column_index, a, bold)
            removals_sheet.write(0, column_index, a, bold)
            total_commits_sheet.write(0, column_index, a, bold)

        current_row = 1

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
                ]).select_from(repo_table.join(authors_table, authors_table.c.id == repo_table.c.author_id)).where(
                    and_(
                        repo_table.c.commit_date >= from_date
                        , repo_table.c.commit_date < to_date
                        , repo_table.c.repo_id.in_(repo_ids)
                    )
                ).alias()

            stats_commits_stmt = select(
                [
                    total_commits_stmt.c.author_id.label('author_id'),
                    func.count(total_commits_stmt.c.commit_hash).label('commit_count'),
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
                    func.count(total_commits_stmt.c.commit_hash).label('commit_count')
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
            # map author to column
            # write data
            reports = { x : Report() for x in authors_list}

            print('{} - {}'.format(from_date, to_date))
            for r in report_commits:

                current_report = Report()

                current_report.commits = r['commit_count']
                current_report.additions = r['additions']
                current_report.removals = r['removals']

                reports[r['author_name']] = current_report

            for r in report_merges:
                if r['author_name'] in reports:
                    reports[r['author_name']].merges = r['commit_count']

            period_string = '{} {}'.format(from_date.month, from_date.year)
            commits_sheet.write_string(current_row, 0, period_string)
            additions_sheet.write_string(current_row, 0, period_string)
            removals_sheet.write_string(current_row, 0, period_string)
            total_commits_sheet.write_string(current_row, 0, period_string)

            for k,v in author_to_column.items():
                row = v
                report = reports[k]

                commits_sheet.write_number(current_row, row, report.commits)
                additions_sheet.write_number(current_row, row, report.additions)
                removals_sheet.write_number(current_row, row, report.removals)
                total_commits_sheet.write_number(current_row, row, report.commits + report.merges)

            from_date = to_date
            to_date = add_month(to_date)
            current_row = current_row + 1


    chartCommits = workbook.add_chart({'type': 'line'})
    chartCommitsWithMerges = workbook.add_chart({'type': 'line'})
    chartAdditions = workbook.add_chart({'type': 'line'})
    chartRemovals = workbook.add_chart({'type': 'line'})

    for k,v in author_to_column.items():
        chartCommits.add_series({
            'name':       [commits_sheet.name, 0, v],
            'categories': [commits_sheet.name, 1, 0, current_row-1, 0],
            'values':     [commits_sheet.name, 1, v, current_row-1, v]
        })

        chartAdditions.add_series({
            'name':       [additions_sheet.name, 0, v],
            'categories': [additions_sheet.name, 1, 0, current_row-1, 0],
            'values':     [additions_sheet.name, 1, v, current_row-1, v]
        })

        chartRemovals.add_series({
            'name':       [removals_sheet.name, 0, v],
            'categories': [removals_sheet.name, 1, 0, current_row-1, 0],
            'values':     [removals_sheet.name, 1, v, current_row-1, v]
        })

        chartCommitsWithMerges.add_series({
            'name':       [total_commits_sheet.name, 0, v],
            'categories': [total_commits_sheet.name, 1, 0, current_row-1, 0],
            'values':     [total_commits_sheet.name, 1, v, current_row-1, v]
        })

    chartCommits.set_title ({'name': 'Commits per month'})
    chartCommits.set_x_axis({'name': 'Month'})
    chartCommits.set_y_axis({'name': 'Commits'})

    chartAdditions.set_title ({'name': 'Additions per month'})
    chartAdditions.set_x_axis({'name': 'Month'})
    chartAdditions.set_y_axis({'name': 'Additions'})

    chartRemovals.set_title ({'name': 'Removals per month'})
    chartRemovals.set_x_axis({'name': 'Month'})
    chartRemovals.set_y_axis({'name': 'Removals'})

    chartCommitsWithMerges.set_title ({'name': 'Commits and merges'})
    chartCommitsWithMerges.set_x_axis({'name': 'Month'})
    chartCommitsWithMerges.set_y_axis({'name': 'Commits'})

    chartCommits.set_style(10)
    chartAdditions.set_style(10)
    chartRemovals.set_style(10)
    chartCommitsWithMerges.set_style(10)

    chartCommits.set_size({'width': 720, 'height': 576})
    chartAdditions.set_size({'width': 720, 'height': 576})
    chartRemovals.set_size({'width': 720, 'height': 576})
    chartCommitsWithMerges.set_size({'width': 720, 'height': 576})

    # Insert the chart into the worksheet (with an offset).
    charts_sheet.insert_chart('B2', chartCommits, {'x_offset': 25, 'y_offset': 10})
    charts_sheet.insert_chart('B32', chartAdditions, {'x_offset': 25, 'y_offset': 10})
    charts_sheet.insert_chart('B64', chartRemovals, {'x_offset': 25, 'y_offset': 10})
    charts_sheet.insert_chart('B96', chartCommitsWithMerges, {'x_offset': 25, 'y_offset': 10})

    workbook.close()
