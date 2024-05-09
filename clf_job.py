from typing import Any, Optional
import pandas as pd
import torch
import numpy as np
import os
from pyspark.sql.functions import lit
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, when, isnan
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession, Row
from transformers import pipeline, DistilBertTokenizer
import logging
import constants
from multi_model_inference import MultiModalModel, get_inference_data, run_inference
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
class CLFJob:
    def __init__(
        self,
        bucket: str,
        final_bucket: str,
        minio_client: Any,
        date_folder: Optional[str],
        column: Optional[str],
        model: Optional[str],
        task: str,
    ):
        self.bucket = bucket
        self.final_bucket = final_bucket
        self.minio_client = minio_client
        self.column = column
        self.date = date_folder
        self.classifier = None
        self.task = task
        self.labels = constants.classifier_target_labels
        self.hypothesis_template = constants.classifier_hypothesis_template
        self.model = model
        self.spark = SparkSession.builder \
        .appName("YourAppName") \
        .getOrCreate()
    def perform_clf(self):
        files = self.minio_client.list_objects_names(self.bucket, self.date)
        for file in files:
            filename = file.split(".")[0]

            if self.final_bucket != self.bucket and self.minio_client.check_obj_exists(
                self.final_bucket, file
            ):
                logging.warning(
                    f"File {file} ignored as it already present in destination"
                )
                continue
            logging.info(f"Starting inference for {file}")
            df = self.minio_client.read_df_parquet(bucket=self.bucket, file_name=file)
            if df.empty:
                logging.warning(f"File {file} is empty")
                continue
            if self.task == "zero-shot-classification":
                df = self.get_inference(df=df)
            elif self.task == "text-classification":
                df = self.get_inference_for_column(df=df)
            elif self.task == "both":
                df = self.get_inference(df=df)
                df = self.get_inference_for_column(df=df)
            else:
                raise ValueError(f'Task "{self.task}" is not available')

            self.minio_client.save_df_parquet(self.final_bucket, filename, df)

    def get_inference(self, df):
        classifier = self.maybe_load_classifier(task="zero-shot-classification")
        print("Classifier loaded")

        # Define the column to use for classification
        column_name = "product" if not self.column else self.column

        # Fill missing values in title, description, and name columns and create a new column
        df = df.withColumn(column_name, when(col("title").isNull(), col("description"))
                                        .when(col("name").isNull(), col("title"))
                                        .otherwise(col("name")))

        # Fill missing values in the new column
        df = df.withColumn(column_name, when(isnan(col(column_name)), "").otherwise(col(column_name)))

        # Filter rows with non-empty values in the specified column
        df = df.filter(col(column_name) != "")

        # Collect inputs for inference
        inputs = [row[column_name] for row in df.collect()]

        # Perform inference
        results = classifier(inputs, self.labels, hypothesis_template=self.hypothesis_template)

        # Extract labels and scores from results
        labels = [result['labels'][0] if result else None for result in results]
        scores = [result['scores'][0] if result else None for result in results]

        # Add labels and scores as new columns to the DataFrame
        df = df.withColumn("label_product", when(col(column_name) != "", labels).otherwise(None).cast(FloatType()))
        df = df.withColumn("score_product", when(col(column_name) != "", scores).otherwise(None).cast(FloatType()))

        return df


    def get_inference_for_mmm(self, df: DataFrame, bucket_name) -> DataFrame:
        df = df.filter(col("image_path").isNotNull())

        if df.count() == 0:
            print("ITS 0 so simply returned, no classification performed")
            return df
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        #filtered_df = df.filter(col("image_path").contains("https"))
        for column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast("string"))
        # Define the schema for the DataFrame
        ad_schema = StructType([
            StructField("url", StringType(), True),
            StructField("title", StringType(), True),
            StructField("text", StringType(), True),
            StructField("domain", StringType(), True),
            StructField("retrieved", StringType(), True),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("image", StringType(), True),
            StructField("production_data", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("seller", StringType(), True),
            StructField("seller_type", StringType(), True),
            StructField("seller_url", StringType(), True),
            StructField("location", StringType(), True),
            StructField("ships to", StringType(), True),
            StructField("id", StringType(), True),
            StructField("image_path", StringType(), True),
            StructField("predicted_label", IntegerType(), True)
        ])

        classifier = self.maybe_load_classifier(task="zero-shot-classification")

        @pandas_udf(ad_schema, PandasUDFType.GROUPED_MAP)
        def convert_to_pandas(key, pdf):
            bucket_name = "local"
            minio_client = None
            indices = ~pdf["image_path"].isnull()
            inference_data = get_inference_data(pdf[indices], minio_client, bucket_name)
            predictions =  run_inference(classifier, inference_data,device)
            pdf.loc[indices, 'predicted_label'] = predictions
            return pdf
        converted_df = df.groupby("image").apply(convert_to_pandas)
        converted_df.show()
        return converted_df
    
    def get_inference_for_column(self, df):
        labels_map = {"LABEL_0": 0, "LABEL_1": 1}

        # Load the classifier
        classifier = self.maybe_load_classifier(task="text-classification")

        # Define the column to use for classification
        column_name = "product" if not self.column else self.column

        # Fill missing values in title, description, and name columns and create a new column
        df = df.withColumn(column_name, when(col("title").isNull(), col("description"))
                                        .when(col("name").isNull(), col("title"))
                                        .otherwise(col("name")))

        # Fill missing values in the new column
        df = df.withColumn(column_name, when(isnan(col(column_name)), "").otherwise(col(column_name)))

        # Filter rows with non-empty values in the specified column
        df = df.filter(col(column_name) != "")

        # Collect inputs for inference
        inputs = [row[column_name] for row in df.collect()]

        # Perform inference
        results = classifier(inputs)

        # Extract labels and scores from results
        labels = [labels_map[result['label']] if result and result['label'] in labels_map else None for result in results]
        scores = [result['score'] if result else None for result in results]

        # Add labels and scores as new columns to the DataFrame
        df = df.withColumn("label", when(col(column_name) != "", labels_map[col("label")]).otherwise(None).cast(FloatType()))
        df = df.withColumn("score", when(col(column_name) != "", scores).otherwise(None).cast(FloatType()))

        # Print the number of rows for which inference is completed
        print(f"Inference completed for {len(labels)} rows")

        return df

    def maybe_load_classifier(self, task: Optional[str]):
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        if self.task == "text-classification":
            model = self.model or "julesbarbosa/wildlife-classification"
            if not self.classifier:
                self.classifier = pipeline(
                    self.task,
                    model=model,
                    device=device,
                    use_auth_token=os.environ["HUGGINGFACE_API_KEY"],
                )
            return self.classifier
        elif self.task == "zero-shot-classification":
            model = self.model or "facebook/bart-large-mnli"
            if not self.classifier:
                self.classifier = pipeline(
                    self.task,
                    model=model,
                    device=device,
                    use_auth_token=os.environ["HUGGINGFACE_API_KEY"],
                )
            return self.classifier
        elif self.task == "multi-model":
            # Initialize an empty model
            loaded_model = MultiModalModel(num_labels=2)
            # Load the state dictionary
            if self.minio_client:
                model_load_path = "./model.pth"
                self.minio_client.get_model("multimodal", "model.pth", model_load_path)
            else:
                model_load_path = "./model/model.pth"

            # Check device
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

            # Load the model weights
            if device == torch.device("cpu"):
                loaded_model.load_state_dict(
                    torch.load(model_load_path, map_location=device), strict=False
                )
            else:
                loaded_model.load_state_dict(torch.load(model_load_path), strict=False)

            # Move model to evaluation mode and to the device
            loaded_model.eval()
            loaded_model = loaded_model.to(device)
            self.classifier = loaded_model
            return self.classifier
        elif self.task == "both":
            if task == "text-classification":
                model = "julesbarbosa/wildlife-classification"
            else:
                model = "facebook/bart-large-mnli"
            return pipeline(
                task,
                model=model,
                device=device,
                use_auth_token=os.environ["HUGGINGFACE_API_KEY"],
            )

    @staticmethod
    def get_label(x):
        if x["label_product"] and x["label_description"]:
            return None
        elif x["label_product"]:
            return x["label_description"]
        elif x["label_description"]:
            return x["label_product"]
        else:
            if x["score_description"] > x["score_product"]:
                return x["label_description"]
            else:
                return x["label_product"]
