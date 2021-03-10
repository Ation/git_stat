import xlsxwriter

def get_column_name(index):
    symbols = [ chr(x) for x in range(ord('A'), ord('Z')+1) ]
    current = index % len(symbols)
    leftover = index - current
    result = symbols[current]
    if leftover == 0:
        return result
    return get_column_name(int(leftover/len(symbols))-1) + result

def get_cell_name(row, column):
    return '{}{}'.format(get_column_name(column), int(row+1))

class ExcelGenerator():
    def __init__(self, outputFileName=None):
        self.file_name = outputFileName

    def GenerateReport(self, fullReport):
        if self.file_name is None:
            self.file_name = 'report.xlsx'
        workbook = xlsxwriter.Workbook(self.file_name)
        bold = workbook.add_format({'bold': 1})

        summary_sheet = workbook.add_worksheet('summary')
        charts_sheet = workbook.add_worksheet('charts')
        commits_sheet = workbook.add_worksheet('commits')
        additions_sheet = workbook.add_worksheet('additions')
        removals_sheet = workbook.add_worksheet('removals')
        total_commits_sheet = workbook.add_worksheet('commits_with_merges')

        # write summary on repo
        summary_table_options = {
            'columns' :
            [
                {'header' : 'Repo name'},
                {'header' : 'Repo URL'}
            ]
        }
        repo_list = fullReport.getRepoInfo()

        description_row = 1
        start_row = 3
        start_column = 1

        end_row = start_row + len(repo_list)
        end_column =  2

        cell_format = workbook.add_format({
            'bold':     True,
            'align':    'center',
            'valign':   'vcenter',
        })

        summary_sheet.merge_range('{}:{}'.format(
            get_cell_name(row=description_row, column=start_column), get_cell_name(row=description_row, column=end_column))
            , 'Repository used to collect data', cell_format)

        name_column=get_column_name(start_column)
        url_column=get_column_name(end_column)
        summary_sheet.set_column(f'{name_column}:{name_column}', 20)
        summary_sheet.set_column(f'{url_column}:{url_column}', 60)


        summary_repo_table = summary_sheet.add_table('{}:{}'.format(
            get_cell_name(row=start_row, column=start_column), get_cell_name(row=end_row, column=end_column)), options=summary_table_options)

        current_row = start_row + 1

        for r in repo_list:
            summary_sheet.write_row(get_cell_name(row=current_row, column=start_column), [r.name, r.link])
            current_row = current_row + 1

        # write headings
        commits_sheet.write_string(0, 0, 'Month', bold)
        additions_sheet.write_string(0, 0, 'Month', bold)
        removals_sheet.write_string(0, 0, 'Month', bold)
        total_commits_sheet.write_string(0, 0, 'Month', bold)

        author_to_column = {}
        for c, a in enumerate(fullReport.getAuthors()):
            column_index = c+1
            author_to_column[a] = column_index

            commits_sheet.write(0, column_index, a, bold)
            additions_sheet.write(0, column_index, a, bold)
            removals_sheet.write(0, column_index, a, bold)
            total_commits_sheet.write(0, column_index, a, bold)

        current_row = 1

        ranges = fullReport.getSortedRanges()
        for date_range in ranges:
            period_string = '{} {}'.format(date_range.start.month, date_range.start.year)
            commits_sheet.write_string(current_row, 0, period_string)
            additions_sheet.write_string(current_row, 0, period_string)
            removals_sheet.write_string(current_row, 0, period_string)
            total_commits_sheet.write_string(current_row, 0, period_string)

            for author_name,colunm_index in author_to_column.items():
                authorReport = fullReport.getReportEntry(date_range, author_name)

                commits_sheet.write_number(current_row, colunm_index, authorReport.commits)
                additions_sheet.write_number(current_row, colunm_index, authorReport.additions)
                removals_sheet.write_number(current_row, colunm_index, authorReport.removals)
                total_commits_sheet.write_number(current_row, colunm_index, authorReport.commits + authorReport.merges)

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
