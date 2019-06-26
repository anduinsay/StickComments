#!/usr/bin/env python
"""
This is main luigi job run from command line.
It is composed of a wrapper Task
SubredditTask will create 1 GamingTrack task per month
"""

import datetime
import luigi
from tasks import GamingTrack

class SubredditTask(luigi.WrapperTask):
    subreddit = luigi.Parameter(default='gaming')

    def requires(self):
        """
        Iterate over month for callin GamingTrack
        """
        months = self.get_month_list()
        for month in months:
            yield GamingTrack(subreddit=self.subreddit, month=month)

    def get_month_list(self):
        """
        Creates a list of week start dates by adding 7 days successively
        to the first day of a year
        """
        month_length = datetime.timedelta(months=1)
        
        months = []
        for year in range(2008, 2015):
            beginning = datetime.date(year, 01, 01)
            for month_number in range(1, 13):
                month_start = beginning + month_length * month_number
                months.append(month_start.isoformat())

        return months

if __name__ == '__main__':
    subreddits = ['games', 'gaming']

    luigi.build([SubredditTask(subreddit=subreddit) for subreddit in subreddits])
