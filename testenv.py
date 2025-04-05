from dotenv import load_dotenv
import os

load_dotenv()
print(os.getenv("POSTGRES_USERNAME"))
print(os.getenv("POSTGRES_PORT"))
print(os.getenv("POSTGRES_PASSWORD"))
print(os.getenv("POSTGRES_DEFAULT_DB"))
print(os.getenv("SPARK_MASTER_URL"))