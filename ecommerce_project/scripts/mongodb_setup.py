from pymongo import MongoClient
import json
import os
from datetime import datetime
import glob

# --- Setup MongoDB Connection ---
client = MongoClient('mongodb://localhost:27017/')
db = client['ecommerce_db']

# --- Base directory setup ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'ecommerce_data')

# --- Create Indexes ---
db.users.create_index([("user_id", 1)], unique=True)
db.products.create_index([("product_id", 1)], unique=True)
db.products.create_index([("category_id", 1)])
db.categories.create_index([("category_id", 1)], unique=True)
db.transactions.create_index([("transaction_id", 1)], unique=True)
db.transactions.create_index([("user_id", 1), ("timestamp", -1)])
db.transactions.create_index([("session_id", 1)])

# --- Data Loading Function ---
def load_json_to_mongo(file_path, collection_name):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        collection = db[collection_name]
        collection.insert_many(data)
        print(f"Loaded {len(data)} documents into '{collection_name}' collection.")
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
    except json.JSONDecodeError:
        print(f"❌ JSON decode error in file: {file_path}")
    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")

# --- Load JSON Data ---
load_json_to_mongo(os.path.join(DATA_DIR, 'users.json'), 'users')
load_json_to_mongo(os.path.join(DATA_DIR, 'products.json'), 'products')
load_json_to_mongo(os.path.join(DATA_DIR, 'categories.json'), 'categories')
load_json_to_mongo(os.path.join(DATA_DIR, 'transactions.json'), 'transactions')

# --- Aggregation Queries ---

# 1. Top-selling products
pipeline_top_products = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "total_quantity": {"$sum": "$items.quantity"},
        "total_revenue": {"$sum": "$items.subtotal"}
    }},
    {"$lookup": {
        "from": "products",
        "localField": "_id",
        "foreignField": "product_id",
        "as": "product_info"
    }},
    {"$unwind": "$product_info"},
    {"$project": {
        "product_id": "$_id",
        "product_name": "$product_info.name",
        "total_quantity": 1,
        "total_revenue": 1
    }},
    {"$sort": {"total_quantity": -1}},
    {"$limit": 10}
]

top_products = list(db.transactions.aggregate(pipeline_top_products))
print("\n🔝 Top 10 Selling Products:")
for p in top_products:
    print(f"{p['product_name']}: {p['total_quantity']} units, ${p['total_revenue']:.2f}")

# 2. User segmentation by purchase frequency
pipeline_user_segmentation = [
    {"$group": {
        "_id": "$user_id",
        "purchase_count": {"$sum": 1}
    }},
    {"$bucket": {
        "groupBy": "$purchase_count",
        "boundaries": [0, 1, 5, 10, float('inf')],
        "default": "Other",
        "output": {
            "user_count": {"$sum": 1}
        }
    }},
    {"$project": {
        "segment": {
            "$switch": {
                "branches": [
                    {"case": {"$eq": ["$_id", 0]}, "then": "No Purchases"},
                    {"case": {"$eq": ["$_id", 1]}, "then": "Single Purchase"},
                    {"case": {"$lte": ["$_id", 5]}, "then": "Occasional Buyer"},
                    {"case": {"$lte": ["$_id", 10]}, "then": "Frequent Buyer"}
                ],
                "default": "Loyal Customer"
            }
        },
        "user_count": 1
    }}
]

user_segments = list(db.transactions.aggregate(pipeline_user_segmentation))
print("\n👥 User Segmentation by Purchase Frequency:")
for s in user_segments:
    print(f"{s['segment']}: {s['user_count']} users")


