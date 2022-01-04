import pandas as pd
from pymongo import MongoClient, UpdateOne, UpdateMany, InsertOne


class MongoDBUtil:

    def __init__(self, connection_string=None, ip=None, port=None, username=None, password=None, site=None):
        if connection_string:
            self.connection = MongoClient(connection_string)
        else:
            self.connection = MongoClient(host='host', port='port', username='username', password='password', authMechanism='SCRAM-SHA-1')
            if username and password:
                self.connection = MongoClient(host=ip, port=port, username=username, password=password, unicode_decode_error_handler='ignore')
            else:
                self.connection = MongoClient(ip, port, unicode_decode_error_handler='ignore')

    def to_dataframe(
        self,
        db,
        collection,
        find_query={},
    ):
        cursor = self.connection[db][collection].find(find_query)
        return pd.DataFrame.from_records(cursor)

    def upload2mongo(self, df, db, collection, columns=None) -> None:
        """
            columns: 要upload的欄位，格式為list。 e.g., ['col1','col2','col3']
        """
        if columns is None:
            columns = df.columns
        bulk_list = []
        # 存到bulk_list裡面by row
        for idx, row in df.iterrows():
            # update_one: 第一部分是根據什麼找到要更新的對象，第二部份是要更新的內容
            bulk_list.append(InsertOne({x: row[x] for x in columns}))
        self.connection[db][collection].bulk_write(bulk_list)

    def upsert2mongo(self, df, db, collection, matched_columns, update_columns, mode='one') -> None:
        """
            https://docs.mongodb.com/manual/reference/operator/update/setOnInsert/
            matched_columns(setOnInsert): 要match的欄位，格式為list。 e.g., ['col1','col2','col3']
            update_columns(set): 要更新資料的欄位，格式為list。 e.g., ['col4','col5','col6']
            mode分成one或是many，one只會更新match到的第一筆資料，many會跟新match到的所有資料
        """
        bulk_list = []
        records = df.to_dict("records")
        if mode.lower() == 'one':
            for record in records:
                bulk_list.append(UpdateOne({x: record[x] for x in matched_columns}, {"$set": {x: record[x] for x in update_columns}}, upsert=True))
        elif mode.lower() == 'many':
            for record in records:
                bulk_list.append(UpdateMany({x: record[x] for x in matched_columns}, {"$set": {x: record[x] for x in update_columns}}, upsert=True))
        else:
            raise ValueError("Mode should be 'One' or 'many'.")
        self.connection[db][collection].bulk_write(bulk_list)
