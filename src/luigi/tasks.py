'''
Tasks define the operation logic for luigi

Task defined here:
- GamingTrack load data from S3, write output to postgres db
- WritePostgres load data from GamingTrack, send result to postgres db
'''


class GamingTrack(SparkSubmitTask):
'''
This task get a subreddit and month, runs a spark-submit job based on the spark app
'''

    subreddit = luigi.Parameter(default='gaming')
    week = luigi.Parameter(default='2007-11')

    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    ip = config.get('spark', 'ip')
    port = config.get('spark', 'port')

    spark_submit = '/usr/local/spark/bin/spark-submit'
    master = 'spark://{ip}:{port}'.format(ip=ip, port=port)
    app = u'/home/ubuntu/stickcomments/src/spark/process-to-postgres.py'
    packages = 'org.postgresql:postgresql:42.2.5'

    def app_options(self):
        """
        These are appended following spark-submit app.py subreddit 
        """
        return [self.subreddit, self.month]

    def output(self):
        return S3Target('s3a://redditcom/result/{subreddit/{month:02d}'.format(
            subreddit=self.subreddit, month=self.month))

    @property
    def packages(self):
        return ['org.postgresql:postgresql:42.2.5']

class WriteToPG(CopyToTable):
    subreddit = luigi.Parameter(default='gaming')
    week = luigi.Parameter(default='2007-11')
    
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    user = config.get('db', 'user')
    password = config.get('db', 'password')
    host = config.get('db', 'ip')
    port = config.get('db', 'port')
    database = config.get('db', 'database')
    table = 'redditres'

    columns = [
        ('subreddit', 'TEXT'),
        ('month', 'TEXT'),
        ('users','INT'),
	('comments','INT')
    ]

    def input(self):
        return S3Target('s3a://redditcom/result/{subreddit/{month:02d}'.format(
            subreddit=self.subreddit, month=self.month))

    def requires(self):
        return GamingTrack(self.subreddit, self.month)
    
