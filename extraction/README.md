# Extract Reddit Data From Google BigQuery

A Google Cloud Platform JSON key is need for generating files from Reddit

The key can be generated from https://console.cloud.google.com/apis/credentials/serviceaccountkey.


It is not sufficient for processing from S3.

The first constraint here is it must follow download, upload setup, and json key from bigquery has access capacity limit.

The more accessible way is to load from pushshift.io, which use awscli can directly load data to S3 bucket. https://files.pushshift.io/reddit/
