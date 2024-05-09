import unittest
from etl_disk_job import ETLDiskJob
import json
import time
import pandas as pd
import chardet
import pybase64
import os


class TestProcessData(unittest.TestCase):

    # Test1

    # def test_create_df(self):
    #     job = ETLDiskJob("local", None, "data/", True, None, None, None, None)
    #     decompressed_data = job.get_decompressed_file("crawl_data-1699796707793-0.deflate")

    #     cached = []
    #     for line in decompressed_data.splitlines():
    #         json_doc = json.loads(line)
    #         cached.append(json_doc)

    #     df = job.create_df(cached)
    #     df_origin = pd.read_csv("complete_data_w_pred.csv")
    #     assert df["seller"] == df_origin["seller"]
    #     assert df["location"] == df_origin["location"]

    # Test2

    # def test_perform_classification(self):
    #     df = pd.read_csv("test_3.csv")
    #     df = df[0:100]
    #     job = ETLDiskJob("local", None, "data/", None, "multi-model", None, None, None)

    #     df = df[df["title"].notnull()]
    #     df = job.perform_classification(df, None)
    #     df.to_csv("test_pred.csv", index=False)
    #     assert "predicted_label" in df.columns

    # Test3
    
    def test_integration(self):
        job = ETLDiskJob("local", None, "/Users/dhruvnain/Desktop/wildlife_pipeline_on_dataproc/data/", None, "multi-model", None, None, None)
        job.run("", "image_data")
        '''
        files = os.listdir("/Users/dhruvnain/Desktop/wildlife_pipeline_on_dataproc/")
        for file in files:
            try:
                filename = file.split(".")[0]
                folderPath = f"/Users/dhruvnain/Desktop/wildlife_pipeline_2/{filename}spark.csv"
                df = pd.read_csv(folderPath)
                assert not df.empty, f"{filename}.csv is empty."
                assert "predicted_label" in df.columns

            except FileNotFoundError:
                print(f"File not found: {filename}.csv")
            except AssertionError as error:
                print(error)
            except Exception as error:
                print(
                    f"An unexpected error occurred while processing {filename}.csv: {error}"
                )
        '''

if __name__ == "__main__":
    unittest.main()
