import json
import os
import time
import pandas as pd
from db_connection import DBConnection


class LoadData:
    @staticmethod
    def to_dataframe(filelocation):
        """
        This func is used to remove duplicates and call db func's from db_connection
        :return: the bulk upload time, update time, total time taken
        """
        start_time = time.time()
        with open('config.json', 'r') as f:
            config = json.load(f)
        d = DBConnection(config['postgres']['db'], config['postgres']['user'], config['postgres']['passwd'],
                         config['postgres']['host'])
        data = pd.read_csv(filelocation)
        data.sort_values("sku", inplace=True)  # sorts by sku
        data.drop_duplicates(subset="sku",
                             keep=False, inplace=True)  # drop the duplicates
        data.head(100).to_csv(os.getcwd() + '/processed_data/data1.csv', index=False)  # initially only uploading 100 data
        d.bulk_upload_file(os.getcwd() + '/processed_data/data1.csv')
        print("bulk upload time : ", time.time() - start_time)
        update_time = time.time()
        data.to_csv(os.getcwd() + '/processed_data/data2.csv', index=False)
        d.update_table(os.getcwd() + '/processed_data/data2.csv')  # uploading all data
        print("update time: ", time.time() - update_time)
        print("total time taken : ", time.time() - start_time)
        return "added successfully"


if __name__ == "__main__":
    LoadData.to_dataframe()
