from pymongo import MongoClient
from mongo_service.config import DB_URL

client = MongoClient(DB_URL)
db = client['terrorism_data']
collection = db['events']