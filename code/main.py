import os
import pandas as pd


class LoadData:
    @staticmethod
    def preprocess_data(filelocation):
        """
        This func is used to remove duplicates and call db func's from db_connection
        :return: the bulk upload time, update time, total time taken
        """
        data = pd.read_csv(filelocation)
        data.sort_values("sku", inplace=True)  # sorts by sku
        data.drop_duplicates(subset="sku",
                             keep=False, inplace=True)  # drop the duplicates
        data.to_csv(os.getcwd() + '/processed_data/preprocessed.csv', index=False)
        return "added successfully"


if __name__ == "__main__":
    LoadData.preprocess_data()
