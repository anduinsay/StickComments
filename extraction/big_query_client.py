from google.cloud import bigquery
import json

class BigQueryClient:
    """ Extracts data from Google BigQuery
    This class will not function without the json key. 
    """

    query = 'SELECT * FROM `fh-bigquery.reddit_comments.2015_01` '

    def __init__(self, key_path = None):
        if key_path is None:
            key_path = 'key.json'
        self.bigquery_client = bigquery.Client.from_service_account_json(key_path)

    def run_query(self, query, limit = None):
        """
        Performs a BigQuery query.
        """
        if limit is not None:
            query += 'LIMIT ' + str(limit)
        query_job = self.bigquery_client.query(query)  # API request
        return query_job.result()  # Waits for query to finish

    def produce_json_data(self, producer, limit = None):
        """
        Runs a query and converts the result into a json object.
        """
        raw_data = self.run_query(BigQueryClient.query, limit)
        for row in raw_data:
            json_data = self.__data_row_to_json(row)
            producer.send_data(json_data)


    def __data_row_to_json(self, row):
        """
        Converts data inside a row iterator to a json object.
        """
        raw_data = {}
        raw_data["body"] = row.body
        raw_data["score_hidden"] = row.score_hidden
        raw_data["archived"] = row.archived
        raw_data["name"] = row.name
        raw_data["author"] = row.author
        raw_data["author_flair_text"] = row.author_flair_text
        raw_data["downs"] = row.downs
        raw_data["created_utc"] = row.created_utc
        raw_data["subreddit_id"] = row.subreddit_id
        raw_data["link_id"] = row.link_id
        raw_data["parent_id"] = row.parent_id
        raw_data["score"] = row.score
        raw_data["retrieved_on"] = row.retrieved_on
        raw_data["controversiality"] = row.controversiality
        raw_data["gilded"] = row.gilded
        raw_data["id"] = row.id
        raw_data["subreddit"] = row.subreddit
        raw_data["ups"] = row.ups
        raw_data["distinguished"] = row.distinguished
        raw_data["author_flair_css_class"] = row.author_flair_css_class

        return json.dumps(raw_data)
