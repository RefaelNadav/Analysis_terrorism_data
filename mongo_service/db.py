from pymongo import MongoClient
from config import DB_URL

client = MongoClient(DB_URL)
db = client['terrorism_data']
collection = db['events']