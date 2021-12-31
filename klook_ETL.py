import datetime as dt
import pandas as pd
from mongoutil import MongoDBUtil
import requests
import json


class klook_scraper():

    def __init__(self, db, collection):
        CONNECTION_STRING = 'mongodb+srv://zach:kkdaytest@cluster0.rl4wj.mongodb.net/kkday?retryWrites=true&w=majority'
        self.m = MongoDBUtil(connection_string=CONNECTION_STRING)
        self.db = db
        self.collection = collection

    def fetch_then_upload(self) -> None:
        queue = []
        page = 1
        while True:
            url = f'https://www.klook.com/v1/experiencesrv/category/activity?frontend_id_list=19&size=24&start={page}'
            headers = {'accept-language': 'zh_TW', 'currency': 'TWD'}
            r = requests.get(url=url, headers=headers)
            result = json.loads(r.text)['result']['activities']
            result_df = pd.DataFrame(result)
            if not result:
                break
            queue.append(result_df)
            page += 1

        final_result = pd.concat(queue).reset_index(drop=True)
        self.m.upsert2mongo(df=final_result, db=self.db, collection=self.collection, matched_columns=[], update_columns=[])

    def filter_then_export(self) -> None:
        data = self.m.to_dataframe(db=self.db, collection=self.collection)
        data = data[data['review_star'] >= 4]
        now = dt.datetime.now().strftime('%Y-%m-%d')
        pd.to_csv(f'data_{now}.csv')
