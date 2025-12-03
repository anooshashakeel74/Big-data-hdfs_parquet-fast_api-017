from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession
import os
import pandas as pd

app = FastAPI()

INITIAL_STUDENTS = [
    {"name": "Alice", "age": 28},
    {"name": "Bob", "age": 25}
]


@app.get("/")
def root():
    return {"message": "FastAPI is running with Spark + Hadoop"}


@app.get("/hello")
def hello_world():
    return {"message": "Hello World from FastAPI + Spark + Hadoop!"}


def get_spark(app_name="FastAPI_Spark"):
    SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://pyspark-master:7077")
    SPARK_FS = os.getenv("SPARK_FS", "hdfs://namenode:9000")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.hadoop.fs.defaultFS", SPARK_FS)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-client:3.2.1")
        .getOrCreate()
    )
    return spark

def read_or_create_df(spark: SparkSession, hdfs_path: str):

    try:
        df = spark.read.parquet(hdfs_path)

        if df.count() == 0:
            raise Exception("Parquet file found but is empty. Proceeding with initialization.")

    except Exception as e:
        print(f"HDFS file not found or empty at {hdfs_path}. Creating initial data. Error: {e}")

        initial_pd_df = pd.DataFrame(INITIAL_STUDENTS)

        df = spark.createDataFrame(initial_pd_df)

        try:
            df.write.mode("overwrite").parquet(hdfs_path)
            print(f"Successfully initialized and saved {len(INITIAL_STUDENTS)} records to HDFS.")
        except Exception as write_error:
            print(f"CRITICAL: Failed to write initial DataFrame to HDFS. {write_error}")

            empty_pd_df = pd.DataFrame({'name': pd.Series(dtype='str'), 'age': pd.Series(dtype='int64')})
            df = spark.createDataFrame(empty_pd_df)

    return df


@app.get("/students")
def get_students():
    HDFS_PATH = os.getenv("HDFS_PATH", "/user/root/students.parquet")
    spark = get_spark("API_Read_Students")

    df = read_or_create_df(spark, HDFS_PATH)
    data = df.toPandas().to_dict(orient="records")
    return {"students": data}


@app.post("/students")
def add_student(student: dict):
    if 'name' not in student or 'age' not in student:
        raise HTTPException(status_code=400, detail="Student must have 'name' and 'age' fields.")
    try:
        student['age'] = int(student['age'])
    except ValueError:
        raise HTTPException(status_code=400, detail="Age must be an integer.")

    HDFS_PATH = os.getenv("HDFS_PATH", "/user/root/students.parquet")
    spark = get_spark("API_Add_Student")

    df = read_or_create_df(spark, HDFS_PATH)
    existing = df.toPandas()

    new_student = pd.DataFrame([student])

    updated = pd.concat([existing, new_student], ignore_index=True)

    updated_spark = spark.createDataFrame(updated)
    updated_spark.write.mode("overwrite").parquet(HDFS_PATH)

    return {"message": "Student added successfully", "student": student}


@app.get("/avg-age")
def get_avg_age():
    HDFS_PATH = os.getenv("HDFS_PATH", "/user/root/students.parquet")
    spark = get_spark("API_Avg_Age")

    df = read_or_create_df(spark, HDFS_PATH)

    if df.count() == 0:
        return {"average_age": []}

    df.createOrReplaceTempView("students")

    avg_df = spark.sql("SELECT name, AVG(age) AS avg_age FROM students GROUP BY name")

    data = avg_df.toPandas().to_dict(orient="records")

    return {"average_age": data}