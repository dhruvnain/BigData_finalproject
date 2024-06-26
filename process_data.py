import logging
from typing import Dict, List, Optional
from pyspark.sql import DataFrame
import datamart_geo
from io import BytesIO
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, first, udf
from pyspark.sql.functions import monotonically_increasing_id
from bs4 import BeautifulSoup
from mlscraper.html import Page
import numpy as np
from numpy import asarray
import pandas as pd
from PIL import Image
import requests
from typing import Any
import re
import pickle
import uuid
import os
import shutil
import tempfile
from pyspark.sql.functions import when, udf


from create_metadata import (
    open_scrap,
    get_sintax_opengraph,
    get_sintax_dublincore,
    get_dict_json_ld,
    get_dict_microdata
)
from clf_job import CLFJob
import extruct
import constants
import ftfy

geo_data = datamart_geo.GeoData.download(update=False)


class ProcessData:
    def __init__(self, bloom_filter, minio_client, bucket, task, column, model):
        self.minio_client = minio_client
        self.bloom_filter = bloom_filter
        self.domains = {}
        self.bucket = bucket
        self.task = task
        self.column = column
        self.model = model
        self.spark = SparkSession .builder .appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value") .getOrCreate()
    def open_scrap(self, minio_client: Any, domain: str):
        if domain not in self.domains.keys():
            if self.minio_client:
                obj = minio_client.get_obj("scrapers", "scraper_" + domain)
                scraper = pickle.load(obj)
                self.domains[domain] = scraper
                logging.info(f"{domain} MLscraper loaded")
            elif os.path.exists("scrapers/"):
                scraper = pickle.load("scrapers/scraper_" + domain)
                self.domains[domain] = scraper
                logging.info(f"{domain} MLscraper loaded")
        return self.domains[domain]

    @staticmethod
    def remove_text(text: Optional[str]):
        if text:
            return any(phrase in text for phrase in constants.phrases_to_filter)
        return True

    
    def extract_information_from_docs(self, result: list) :
        def log_processed(raw_count: int, processed_count: int) -> None:
            logging.info(f"{pd.Timestamp.now()}: received {raw_count} articles, total: "
                         f"{processed_count} unique processed")

        cache = []
        count = 0
        hits = len(result)
        for val in result:
            processed = val.get("_source")
            if processed:
                if not ProcessData.remove_text(processed["text"]) and not self.bloom_filter.check_bloom_filter(
                        processed["text"]):
                    count += 1
                    cache.append(processed)
            elif val["content"]:
                count += 1
                cache.append(val)
        log_processed(hits, count)
        df = self.spark.createDataFrame([], schema="")
        if count > 0:
            df = self.create_df(cache)
            if not df.rdd.isEmpty():
                 uuid_udf =self.spark.udf.register("uuid_udf", lambda : str(uuid.uuid4()))
                 df = df.withColumn("id", uuid_udf())
                 df = self.get_location_info(df)
        return df
    

    def create_df(self, ads: list):
        final_dict = []
        for ad in ads:
            dict_df = self.create_dictionary_for_dataframe_extraction(ad)
            final_dict.append(dict_df)
            domain = ad["domain"].split(".")[0]
            if "ebay" in domain:
                extract_dict = dict_df.copy()
                self.add_seller_information_to_metadata(ad["html"], domain, extract_dict, ad["content_type"])
                final_dict.append(extract_dict)
            if self.minio_client and domain in constants.DOMAIN_SCRAPERS:
                try:
                    extract_dict = dict_df.copy()
                    scraper = open_scrap(self.minio_client, domain)
                    extract_dict.update(scraper.get(Page(ad["html"])))
                    if extract_dict.get("product"):
                        extract_dict["name"] = extract_dict.pop("product")
                    final_dict.append(extract_dict)
                except Exception as e:
                    logging.error(f"MLscraper error: {e}")
            try:
                metadata = None
                metadata = extruct.extract(ad["html"],
                                        base_url=ad["url"],
                                        uniform=True,
                                        syntaxes=['json-ld',
                                                    'microdata',
                                                    'opengraph',
                                                    'dublincore'])
            except Exception as e:
                logging.error(f"Exception on extruct: {e}")

            if metadata:
                if metadata.get("microdata"):
                    for product in metadata.get("microdata"):
                        micro = get_dict_microdata(product)
                        if micro:
                            extract_dict = dict_df.copy()
                            extract_dict.update(micro)
                            final_dict.append(extract_dict)
                if metadata.get("opengraph"):
                    open_ = get_sintax_opengraph(metadata.get("opengraph")[0])
                    if open_:
                        extract_dict = dict_df.copy()
                        extract_dict.update(open_)
                        final_dict.append(extract_dict)
                if metadata.get("dublincore"):
                    dublin = get_sintax_dublincore(metadata.get("dublincore")[0])
                    if dublin:
                        extract_dict = dict_df.copy()
                        extract_dict.update(dublin)
                        final_dict.append(extract_dict)
                if metadata.get("json-ld"):
                    for meta in metadata.get("json-ld"):
                        if meta.get("@type") == 'Product':
                            json_ld = get_dict_json_ld(meta)
                            if json_ld:
                                extract_dict = dict_df.copy()
                                extract_dict.update(json_ld)
                                final_dict.append(extract_dict)
            logging.info("extracted metadata from ad")
        
       
        df_metas = self.spark.createDataFrame(final_dict)
        
      
        fix_price_str_udf = udf(ProcessData.fix_price_str)
        fix_currency_udf = udf(ProcessData.fix_currency)
        df_metas = df_metas.withColumn("price", fix_price_str_udf(col("price")))
        df_metas = df_metas.withColumn("currency", fix_currency_udf(col("currency")))
        
   
        df_metas = df_metas.groupBy('url').agg(
            first("title").alias("title"),
            first("text").alias("text"),
            first("domain").alias("domain"),
            first("name").alias("name"),
            first("description").alias("description"),
            first("image").alias("image"),
            first("retrieved").alias("retrieved"),
            first("production_data").alias("production_data"),
            first("category").alias("category"),
            first("price").alias("price"),
            first("currency").alias("currency"),
            first("seller").alias("seller"),
            first("seller_type").alias("seller_type"),
            first("seller_url").alias("seller_url"),
            first("location").alias("location"),
            first("ships to").alias("ships to")
        )
        
        assert_types_udf = udf(ProcessData.assert_types)
        df_metas = df_metas.withColumn("df_metas", assert_types_udf(struct([df_metas[x] for x in df_metas.columns])))  
        print("df_metas created")
        return df_metas

    
    @staticmethod
    def fix_price_str(price):
        if isinstance(price, tuple):
            price = price[1]

        if isinstance(price, str):
            # Remove non-digit characters from the string, except commas and periods
            cleaned_price = re.sub(r"[^\d,.]", "", price)
            cleaned_price = cleaned_price.replace(",", "")
            if cleaned_price:
                extracted_value = float(cleaned_price.replace(",", ""))
                # Check if the extracted value has no fractional part
                if extracted_value.is_integer():
                    extracted_value = int(extracted_value)

                return extracted_value
            return None

        elif isinstance(price, list):
            extracted_prices = []
            for item in price:
                # Remove non-digit characters from each element in the list, except commas and periods
                try:
                    cleaned_item = re.sub(r"[^\d,.]", "", item)
                    cleaned_item = cleaned_item.replace(",", "")
                    if cleaned_item:
                        extracted_value = float(cleaned_item.replace(",", ""))
                        # Check if the extracted value has no fractional part
                        if extracted_value.is_integer():
                            extracted_value = int(extracted_value)

                        extracted_prices.append(extracted_value)
                except TypeError:
                    continue
            if extracted_prices:
                return sum(extracted_prices) / len(extracted_prices)
            else:
                return None
        return price

    @staticmethod
    def fix_currency(x):
        if x is not None and isinstance(x, list):
            x = ", ".join(x)
        return x
    def send_image(self, df: pd.DataFrame, image_folder: Optional[str], bucket_name: str, task: Optional[str],
                   timeout_sec: Optional[int] = 30):
        def send_image_to_minio(row):
            try:
                response = requests.get(row["image"], timeout=timeout_sec)
                img = Image.open(BytesIO(response.content))
                image_array = asarray(img)
                image_path = send(image_array, row["id"])
                image_path = bucket_name + "/" + image_path
                return image_path
            except Exception as e:
                print(f"image error: {e}")
                return None

        def send(image_array, img_id):
            pil_image = Image.fromarray(image_array)
            # Save the image to an in-memory file
            in_mem_file = BytesIO()
            pil_image.save(in_mem_file, format='png')
            in_mem_file.seek(0)
            length = len(in_mem_file.read())
            in_mem_file.seek(0)

            if image_folder:
                file_name = f"{image_folder}/{img_id}.png"
            else:
                file_name = f"{img_id}.png"

            self.minio_client.store_image(image=in_mem_file, file_name=file_name, length=length, bucket_name=bucket_name)
            return file_name

        # if task:
        #     sample_indices_0 = df[df["pred"] == 0].sample(int(len(df[df["pred"] == 0]) * 0.2)).index
        #     sample_indices_1 = df[df["pred"] == 1].index
        #     df["sample_image"] = False  # Initialize the column with "false"
        #     df.loc[sample_indices_0, "sample_image"] = True  # Set "true" for sample indices
        #     df.loc[sample_indices_1, "sample_image"] = True
        # else:
        # df["sample_image"] = True
        send_image_to_minio_udf = udf(send_image_to_minio, StringType())
        df = df.withColumn("minio_image_path", send_image_to_minio_udf(struct([df[col] for col in df.columns])))
        return df
    '''
    @staticmethod
    def save_image_local(df, image_folder):
        def save_image(row):
            try:
                response = requests.get(row["image"], timeout=30)
                img = Image.open(BytesIO(response.content))
                image_path = os.path.join(image_folder, f"{row['id']}.png")
                img.save(image_path)  # Save the image locally
                return image_path
            except Exception as e:
                print(f"image error: {e}")
                return None
        save_image_spark_udf = udf(save_image, StringType())
        df = df.withColumn("image_path", save_image_spark_udf(struct([df[col] for col in df.columns])))
        return df
    '''
    @staticmethod
    def save_image_local(df, image_folder):
        # Function to download and save an image
        def download_and_save_image(image_url, image_id):
            try:
                # Create image path
                image_path = os.path.join(image_folder, f"{image_id}.png")
                # Check if image already exists
                if not os.path.exists(image_path):
                    # Download and save the image only if it doesn't exist
                    response = requests.get(image_url, timeout=30)
                    img = Image.open(BytesIO(response.content))
                    img.save(image_path)
                return image_path
            except Exception as e:
                print(f"image error: {e}")
                return None

        # Define a UDF
        download_and_save_image_udf = udf(download_and_save_image, StringType())
        df = df.withColumn("unique_id", monotonically_increasing_id())
        image_df = df.select("image", "unique_id").dropDuplicates(["image"]).withColumnRenamed("unique_id", "first_unique_id")
        image_df = image_df.withColumn("image_path", download_and_save_image_udf(col("image"), col("first_unique_id")))
        df_alias = df.alias("df")
        image_df_alias = image_df.alias("image_df")

        # Perform the join using aliases and fully qualified column names
        df = df_alias.join(image_df_alias, col("df.image") == col("image_df.image"), "left_outer").select(
            col("df.url"), col("df.title"), col("df.text"), col("df.domain"), col("df.retrieved"), 
            col("df.name"), col("df.description"), col("df.production_data"), col("df.category"), 
            col("df.price"), col("df.currency"), col("df.seller"), col("df.seller_type"), col("df.seller_url"), 
            col("df.location"), col("df.ships to"), col("df.id"), col("df.image"), col("image_df.image_path")
        )   
        #print("COL5: ",df.columns)
        return df
    
    @staticmethod
    def get_location_info(df):
        def resolve_location(name):
            if name:
                    parts = name.split(", ")
                    for part in parts:
                        result = geo_data.resolve_name(part.strip())  # Remove leading/trailing spaces and resolve each part
                        if result:
                            return result
            return None
            
        df = df.withColumn("location", when(df["location"] == "None", None)
                                .otherwise(df["location"]))
        df = df.withColumn("location", when(df["location"] == "US", "USA")
                                        .otherwise(df["location"]))
        df = df.withColumn("location", when(df["location"] == "GB", "Great Britain")
                                        .otherwise(df["location"]))
        df = df.drop("loc")
        return df

    @staticmethod
    def assert_types(df):
        expected_dtypes = {
            "title": str,
            "text": str,
            "domain": str,
            "name": str,
            "description": str,
            "image": str,
            "retrieved": str,
            "production_data": str,
            "category": str,
            "price": float,
            "currency": str,
            "seller": str,
            "seller_type": str,
            "seller_url": str,
            "location": str,
            "ships to": str,
        }

        # Convert each column to the expected data type
        for column, expected_dtype in expected_dtypes.items():
            if expected_dtype == str:
                df[column] = df[column].astype(expected_dtype)
                try:
                    df[column] = df[column].apply(
                        lambda x: x.encode('utf-8', 'surrogateescape').decode('iso-8859-15') if isinstance(x,
                                                                                                           str) else x)
                except UnicodeEncodeError as e:
                    print(column)
                    print(e)
                    df[column] = ""
            else:
                df[column] = df[column].astype(expected_dtype)
        return df

    def run_classification(self, df: DataFrame, bucket_name: Optional[str]) -> DataFrame:
        logging.info(self.task)
        classifier_job = CLFJob(bucket=self.bucket, final_bucket=self.bucket, minio_client=self.minio_client,
                                date_folder=None, task=self.task, model=self.model, column=self.column)
        if self.task == "zero-shot-classification":
            df = classifier_job.get_inference(df)
        elif self.task == "text-classification":
            df = classifier_job.get_inference_for_column(df)
        elif self.task == "multi-model":
            if "image_path" not in df.columns:
                folder_path= "/Users/dhruvnain/Desktop/wildlife_pipeline_on_dataproc/image_data"
                os.makedirs(folder_path, exist_ok=True)
                temp_dir = tempfile.mkdtemp(dir=folder_path)
                df = ProcessData.save_image_local(df, temp_dir)
                df = classifier_job.get_inference_for_mmm(df, bucket_name)
                #shutil.rmtree(temp_dir)
            else:
                df = classifier_job.get_inference_for_mmm(df, bucket_name)
        elif self.task == "both":
            df = classifier_job.get_inference(df)
            df = classifier_job.get_inference_for_column(df)
        else:
            raise ValueError("Task is either text-classification or zero-shot-classification")
        return df

    @staticmethod
    def create_dictionary_for_dataframe_extraction(ad):
        dict_df = {
            "url": ad["url"],
            "title": ad["title"],
            "text": ad["text"],
            "domain": ad["domain"],
            "retrieved": ad["retrieved"],
            "name": None,
            "description": None,
            "image": None,
            "production_data": None,
            "category": None,
            "price": None,
            "currency": None,
            "seller": None,
            "seller_type": None,
            "seller_url": None,
            "location": None,
            "ships to": None,
        }
        return dict_df

    @staticmethod
    def maybe_fix_text(x):
        # fixes Unicode that’s broken in various ways
        return ftfy.fix_text(x) if isinstance(x, str) else x

    def add_seller_information_to_metadata(self, domain: str, metadata: dict, soup):
        if 'ebay' in domain:
            seller_username, location = self.extract_seller_info_for_ebay(soup)
            metadata["seller"] = seller_username
            metadata["location"] = location

    @staticmethod
    def extract_seller_info_for_ebay(soup):

        # Parse the HTML content
        # soup = BeautifulSoup(page_html, 'html.parser')
        # Find the div element with the specified class
        seller_info_div = soup.find('div', class_='ux-seller-section__item--seller')

        if seller_info_div:
            # Extract the seller's username
            seller_username = seller_info_div.a.span.get_text(strip=True)
        else:
            logging.error("Seller username not found.")
            seller_username = ""

        shipping_location_element = soup.find('div', {
            'class': 'ux-labels-values col-12 ux-labels-values--itemLocation'})

        if shipping_location_element:
            # Extract the shipping location text
            shipping_location_text = shipping_location_element.get_text()
            shipping_location = shipping_location_text.split(':')[-1].strip()
            return seller_username, shipping_location
        else:
            logging.error("Shipping location not found.")
            return seller_username, ""


    @staticmethod
    def get_parser(content_type):
        if 'text/xml' in content_type:
            parser = 'xml'
        elif 'text/html' in content_type:
            parser = 'html.parser'
        elif 'x-asp' in content_type or 'xhtml+xml' in content_type:
            parser = 'lxml'
        elif 'text/plain' in content_type:
            parser = 'plain'
        else:
            return None