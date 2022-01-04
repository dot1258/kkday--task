import datetime as dt
import pandas as pd
from modules.mongoutil import MongoDBUtil
import requests
import json
import re
import time
import random
import numpy as np
import logging


class klook_scraper():

    def __init__(self):
        CONNECTION_STRING = 'mongodb+srv://zach:kkdaytest@cluster0.rl4wj.mongodb.net/kkday?retryWrites=true&w=majority'
        self.m = MongoDBUtil(connection_string=CONNECTION_STRING)
        self.db = 'klook'
        self.updated_ts = dt.datetime.now()

    def extract(self) -> None:
        try:
            queue = []

            # Start from page 1.
            page = 1

            # Fetch 100 activities for every time.
            fetch_count = 100

            # Call api and fetch the data of activities.
            while True:
                url = f'https://www.klook.com/v1/experiencesrv/category/activity?frontend_id_list=19&size={fetch_count}&start={page}'
                headers = {'accept-language': 'zh_TW', 'currency': 'TWD'}
                r = requests.get(url=url, headers=headers)
                result = json.loads(r.text)['result']['activities']
                result_df = pd.DataFrame(result)
                if not result:
                    break
                queue.append(result_df)
                page += 1

            final_result = pd.concat(queue).reset_index(drop=True)
            final_result['updated_ts'] = self.updated_ts
            final_result['review_star'] = final_result['review_star'].astype(float)
            final_result['review_count'] = final_result['review_hint'].apply(lambda x: re.findall(r'([0-9]*)\s則評價', x))
            final_result = final_result.explode('review_count')
            final_result['review_count'] = final_result['review_count'].astype(np.float).astype("Int64")
            final_result['activity_id'] = final_result['activity_id'].astype(str)
            # Upload to activity
            self.m.upsert2mongo(df=final_result[['activity_id', 'title', 'review_star', 'updated_ts']],
                                db=self.db,
                                collection='activity',
                                matched_columns=['activity_id'],
                                update_columns=['title', 'review_star', 'updated_ts'])
            logging.info("Updated collection 'activity' successfully.")

            review_list = final_result[['activity_id', 'review_count']]
            review_list = review_list[pd.isna(review_list['review_count']) == 0]
            review_list = dict(zip(review_list['activity_id'], review_list['review_count']))

            review_queue = []
            review_headers = {
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
                'accept-language': 'zh_TW',
                'currency': 'TWD'
            }
            try:
                for activity, review_count in review_list.items():
                    page = 1
                    if review_count < fetch_count:
                        review_url = f'https://www.klook.com/v1/usrcsrv/activities/{activity}/reviews?page={page}\
    &limit={review_count}&star_num=&lang=&sort_type=0&only_image=false&preview=0'

                    else:
                        review_url = f'https://www.klook.com/v1/usrcsrv/activities/{activity}/reviews?page={page}\
    &limit={fetch_count}&star_num=&lang=&sort_type=0&only_image=false&preview=0'

                    session = requests.Session()
                    review_r = session.get(url=review_url, headers=review_headers)
                    review_result_df = pd.DataFrame(json.loads(
                        review_r.text)['result']['item'])[['id', 'activity_id', 'author_id', 'rating', 'content', 'date']]

                    real_review_count = json.loads(review_r.text)['result']['total']
                    count_remaining = real_review_count - fetch_count
                    review_queue.append(review_result_df)
                    time.sleep(random.randint(5, 10))
                    page += 1

                    while count_remaining > 0:
                        if count_remaining < fetch_count:
                            review_url = f'https://www.klook.com/v1/usrcsrv/activities/{activity}/reviews?page={page}\
    &limit={count_remaining}&star_num=&lang=&sort_type=0&only_image=false&preview=0'

                            break
                        else:
                            review_url = f'https://www.klook.com/v1/usrcsrv/activities/{activity}/reviews?page={page}\
    &limit={fetch_count}&star_num=&lang=&sort_type=0&only_image=false&preview=0'

                        session = requests.Session()
                        review_r = session.get(url=review_url, headers=review_headers)
                        review_result_df = pd.DataFrame(json.loads(
                            review_r.text)['result']['item'])[['id', 'activity_id', 'author_id', 'rating', 'content', 'date']]

                        real_review_count = json.loads(review_r.text)['result']['total']
                        review_queue.append(review_result_df)
                        count_remaining -= fetch_count
                        page += 1
                        time.sleep(random.randint(5, 10))
            except Exception as e:
                logging.critical(f'IP is suspended temporarily, error: {e}')

                review_result = pd.concat(review_queue)
                review_result[['id', 'activity_id']] = review_result[['id', 'activity_id']].astype(str)
                review_result.rename(columns={'date': 'created_ts'})
                review_result['updated_ts'] = dt.datetime.now()
                self.m.upsert2mongo(df=review_result,
                                    db='klook',
                                    collection='review',
                                    matched_columns=['id', 'activity_id'],
                                    update_columns=['author_id', 'rating', 'content', 'updated_ts'])
        except Exception as e:
            logging.critical(e)

    def transform_then_load(self) -> None:
        try:
            data = self.m.to_dataframe(db=self.db, collection='activity')
            data = data[(data['review_star'] < 4) & (data['updated_ts'] == self.updated_ts)]
            now = dt.datetime.now().strftime('%Y-%m-%d')
            data.to_csv(f'data_{now}.csv')
        except Exception as e:
            logging.critical(e)


if __name__ == '__main__':
    ks = klook_scraper()
    ks.extract()
    ks.transform_then_load()
