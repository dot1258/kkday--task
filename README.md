# klook scraper implementation

## Run the ETL pipeline using Airflow.

- step 1: Fetch the data from klook using scraper, and upload to Atlas MongoDB.

  - activities

    - Schema:\
      {\
       'activity_id': string,\
       'review_star': float,\
       'title': string,\
       'update_ts': datetime\
      }

    1. The activity list is js-rendered, so I requested the API to extract the data,
       and used language as 'zh-TW' (default = en-US) and currency as 'NTD' (default = HKD) described in headers.
    2. Transform the data from json to dataframe, add datetime at now as 'update_ts' column for next step filtering.
    3. Uploaded to MonggoDB (collection name = 'activity').

    **Note: MongoDB API config should be masked for database security.**

  - reviews

    - Schema:\
        {\
         'activity_id': string,\
         'id': string,\
         'author_id': string,\
         'content': string,\
         'rating': int,\
         'update_ts': datetime\
        }

    1. Requested the API using activity_id, set limit as 100.
    2. Uploaded to MongoDB (collection name = 'review').

    **Note: This part would be IP-banned if requesting too many times. Could use Scrapy instead to implement random IP scraping.**

- step 2:

  1. Fetched data from MongoDB.
  2. Filter data using 'review_star' and 'update_ts'.
  3. Exported to CSV file.
