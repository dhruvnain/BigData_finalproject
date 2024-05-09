from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
# 创建SparkSession
files = os.listdir('/Users/dhruvnain/Desktop/wildlife_pipeline_on_dataproc/dataframe_folder')
files= [file for file in files if file.endswith(".csv")]
print(files)
for file in files:
    print("Reading File",file)
    df = pd.read_csv(f'/Users/dhruvnain/Desktop/wildlife_pipeline_on_dataproc/dataframe_folder/{file}')
    print(df.head)
























'''
spark = SparkSession.builder \
    .appName("Example") \
    .getOrCreate()

df = spark.read.csv("/Users/dhruvnain/Desktop/wildlife_pipeline_2/Book1.csv", header=True)
# Filter out the records where the image column is not None
final_filename = "new_king.csv"
# Create a window specification
windowSpec = Window.partitionBy("URL").orderBy(F.when(F.col("image") != "None", 0).otherwise(1))

# Add a row number for each row in the partition, prioritizing rows where 'image' is not 'None'
df_with_row_number = df.withColumn("row_number", F.row_number().over(windowSpec))
df_filtered = df_with_row_number.filter(F.col("row_number") == 1).drop("row_number")
# Filter to keep only the first row of each URL, which should have the non-None image
#df_filtered = df_with_row_number.filter(F.col("row_number") == 1).drop("row_number")

#filtered_df = df.filter(df.image != 'None')
df_filtered.coalesce(1).write.csv(final_filename, mode="overwrite", header=True)

# Show the result
df_filtered.show()


spark = SparkSession.builder \
    .appName("Example") \
    .getOrCreate()

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
        StructField("image_path", StringType(), True)
])
@pandas_udf(ad_schema, PandasUDFType.GROUPED_MAP)
def convert_to_pandas(pdf):
    # This UDF receives a Pandas DataFrame for each group and returns it
    return pdf
#.option("mode", "DROPMALFORMED")
# Read the CSV file into a pandas DataFrame
#df_pandas = spark.read_csv("/Users/dhruvnain/Desktop/wildlife_pipeline_2/index.csv/part-00000-04fc8f86-1bf0-40f4-839d-6d07348e3ee2-c000.csv", header=0)
df_pandas = spark.read.format("csv").option("header", "true").schema(ad_schema).load("/Users/dhruvnain/Desktop/wildlife_pipeline_2/index.csv/part-00000-04fc8f86-1bf0-40f4-839d-6d07348e3ee2-c000.csv")
finalfilename = "converted1.csv"
df_pandas.coalesce(1).write.csv(finalfilename, mode="overwrite", header=True)
converted_df = df_pandas.groupby("url").apply(convert_to_pandas)
print("ROWS: ",converted_df.count())
print("Columns: ",len(converted_df.columns))
finalfilename = "converted.csv"
converted_df.coalesce(1).write.csv(finalfilename, mode="overwrite", header=True)
#df_pandas.to_csv(finalfilename, index=False)

print("DataFrame size before drop:", df_pandas.shape)
#print(df_pandas['image'])
# Drop rows where the 'image' column is exactly 'None'
#df_pandas = df_pandas[df_pandas['image'] != 'NaN']
df_pandas.dropna(subset=['image'], inplace=True)
df_pandas = df_pandas[df_pandas['image'] != '']
df_pandas = df_pandas[df_pandas['image'].str.strip() != '']
print("DROP",len(df_pandas['image'].unique()))
print("DataFrame size after drop:", df_pandas.shape)
print('None' in df_pandas['image'].unique())
# Filter rows where any column contains 'https:'
filtered_df = df_pandas[df_pandas.apply(lambda row: row.astype(str).str.contains('https:').any(), axis=1)]
filtered_df = filtered_df[filtered_df['url'].astype(str).str.contains('http')]
def row_contains_phrase(row):
    # Check if any column in the row contains the desired phrase
    return row.apply(lambda x: isinstance(x, str) and "Clean default location" in x).any()
# Apply the function to filter rows based on the presence of the phrase
rows_with_phrase = filtered_df[filtered_df.apply(row_contains_phrase, axis=1)]
# Print the rows that contain the phrase
print(rows_with_phrase)

# If there are many rows, you might want to print only the first few
#print(rows_with_phrase.head())

# Print the count of rows containing the phrase
print(f"Total rows containing the phrase: {len(rows_with_phrase)}")


# Assuming 'None' is a string in the 'image' column, we want to treat it as a missing value
# Replace 'None' with NaN for sorting purposes
#df['image'].replace('None', pd.NA, inplace=True)

# Sort the DataFrame first by 'URL' and then by 'image' column where NaN values are last
#df_sorted = df.sort_values(by=['url', 'image'], na_position='last')

# Drop duplicate 'URL' rows, keeping the first occurrence (which now has the image if available)
#df_filtered = df_sorted.drop_duplicates(subset=['url'], keep='first')
#df_filtered = df_filtered.astype(str)
#df_spark = spark.createDataFrame(df_filtered)
# Save the filtered DataFrame to a new CSV file
#final_filename = "new_king.csv"
#df_spark.coalesce(1).write.csv(final_filename, mode="overwrite", header=True)

#df_pandas.printSchema()
#filtered_df.to_csv("/Users/dhruvnain/Desktop/wildlife_pipeline_2/kuch_nahi_aata.csv", index=False)

# Print out the filtered DataFrame
#print(df_filtered)



# 创建一个示例DataFrame
data = [("Alice s", 85),
        ("Bob s", 70),
        ("Charlie z", 90),
        ("David z", 65)]

columns = ["Name", "Math_Score"]

df = spark.createDataFrame(data, columns)

# 展示原始DataFrame
print("原始DataFrame:")
df.show()


data = [(None,) for _ in range(20)]  # 用元组表示每行的数据
schema = ["location"]

# 手动指定列的数据类型为字符串类型
fields = [StructField("location", StringType(), True)]  # True 表示允许空值

df = spark.createDataFrame(data, StructType(fields))

def resolve_location(name):
            if name:
                parts = name.split(", ")
                for part in parts:
                    result = geo_data.resolve_name(part.strip())  # Remove leading/trailing spaces and resolve each part
                    if result:
                        return result
            return None
        
modify_column_udf = udf(resolve_location, StringType())
df = df.withColumn("loc", modify_column_udf (col("location")))






# 展示 DataFrame
df.show()
'''