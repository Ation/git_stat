from datetime import date
from copy import deepcopy

class AuthorReport(object):
    def __init__(self):
        self.commits = 0
        self.additions = 0
        self.removals = 0
        self.merges = 0

class ReportRange(object):
    def __init__(self, start : date, end : date):
        self.start = start
        self.end = end

class ReportCollector(object):
    def __init__(self, author_list):
        self.authors_list = deepcopy(author_list)
        self.reports = {}

    def getRange(self, report_range : ReportRange):
        if report_range in self.reports:
            return self.reports[report_range]

        report_collection = { x : AuthorReport() for x in self.authors_list }
        self.reports[report_range] = report_collection
        return report_collection

    def getSortedRanges(self):
        return sorted(self.reports.keys(), key=lambda r: r.start)

    def getAuthors(self):
        return deepcopy(self.authors_list)

    def getReportEntry(self, report_range: ReportRange, author:str):
        if author not in self.authors_list:
            return None

        if report_range in self.reports:
            return self.reports[report_range][author]

        return self.getRange(report_range)[author]
