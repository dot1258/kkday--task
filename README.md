# klook scraper implementation

## Run the ETL pipeline using Airflow.

- step 1: Fetch the data from klook using scraper, and upload to Atlas MongoDB.

  - activities

    1. The activity list is js-rendered, so I requested the API to extract the data,
       and used language as 'zh-TW' (default = en-US) and currency as 'NTD' (default = HKD) described in headers.
    2. Transform the data from json to dataframe, add datetime at now as 'update_ts' column for next step filtering.
    3. Uploaded to MonggoDB (collection name = 'activity').

  - reviews
    1. Requested the API using activity_id, set limit as 100.
    2. Uploaded to MongoDB (collection name = 'review').
    ### Note: This part would be IP-banned if requesting too many times. Could use Scrapy instead to implement random IP scraping.

- step 2:

  1. Fetched data from MongoDB.
  2. Filter data using 'review_star' and 'update_ts'.
  3. Exported to CSV file.
