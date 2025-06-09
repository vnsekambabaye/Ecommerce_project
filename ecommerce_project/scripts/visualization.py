
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient

# Setup result directories
agg_folder = '../results'
vis_folder = '../results'
os.makedirs(agg_folder, exist_ok=True)
os.makedirs(vis_folder, exist_ok=True)

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client['ecommerce_db']

# 1. Top Products by Quantity Sold
try:
    pipeline_1 = [
        {"$unwind": "$items"},
        {"$group": {"_id": "$items.product_id", "total_quantity": {"$sum": "$items.quantity"}}},
        {"$sort": {"total_quantity": -1}},
        {"$limit": 10}
    ]
    results_1 = list(db.transactions.aggregate(pipeline_1))
    if results_1:
        df1 = pd.DataFrame(results_1)
        df1.columns = ['product_id', 'total_quantity']
        df1.to_csv(f"{agg_folder}/top_products.csv", index=False)
        plt.figure(figsize=(10, 6))
        sns.barplot(x='product_id', y='total_quantity', data=df1)
        plt.xticks(rotation=45)
        plt.title("Top Products by Quantity Sold")
        plt.tight_layout()
        plt.savefig(f"{vis_folder}/top_products.png")
        plt.close()
    else:
        print("⚠️ No data returned for Top Products")
except Exception as e:
    print(f"❌ Top Products plot failed: {e}")

# 2. Revenue by Category
try:
    pipeline_2 = [
        {"$unwind": "$items"},
        {"$lookup": {
            "from": "products",
            "let": {"pid": "$items.product_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$_id", "$$pid"]}}}
            ],
            "as": "product"
        }},
        {"$unwind": "$product"},
        {"$group": {
            "_id": "$product.category",
            "revenue": {"$sum": {"$multiply": ["$items.price", "$items.quantity"]}}
        }},
        {"$sort": {"revenue": -1}}
    ]
    results_2 = list(db.transactions.aggregate(pipeline_2))
    if results_2:
        df2 = pd.DataFrame(results_2)
        df2.columns = ['category', 'revenue']
        df2.to_csv(f"{agg_folder}/category_revenue.csv", index=False)
        df2.set_index('category').plot.pie(y='revenue', autopct='%1.1f%%', figsize=(8, 8))
        plt.title("Revenue by Category")
        plt.ylabel("")
        plt.tight_layout()
        plt.savefig(f"{vis_folder}/revenue_by_category.png")
        plt.close()
    else:
        print("⚠️ No data returned for Revenue by Category")
except Exception as e:
    print(f"❌ Revenue by Category pie chart failed: {e}")

# 3. Users by Country
try:
    pipeline_3 = [
        {"$group": {"_id": "$country", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    results_3 = list(db.users.aggregate(pipeline_3))
    if results_3:
        df3 = pd.DataFrame(results_3)
        df3.columns = ['country', 'count']
        df3.to_csv(f"{agg_folder}/users_by_country.csv", index=False)
        plt.figure(figsize=(10, 6))
        sns.barplot(x='country', y='count', data=df3)
        plt.xticks(rotation=45)
        plt.title("User Count by Country")
        plt.tight_layout()
        plt.savefig(f"{vis_folder}/users_by_country.png")
        plt.close()
    else:
        print("⚠️ No data returned for Users by Country")
except Exception as e:
    print(f"❌ Users by Country chart failed: {e}")

# 4. Avg Transactions vs. Avg Spending by Country
try:
    pipeline_4 = [
        {"$group": {
            "_id": "$country",
            "avg_spending": {"$avg": "$total_spent"},
            "avg_transactions": {"$avg": "$transaction_count"}
        }},
        {"$sort": {"avg_spending": -1}}
    ]
    results_4 = list(db.user_summary.aggregate(pipeline_4))
    if results_4:
        df4 = pd.DataFrame(results_4)
        df4.columns = ['country', 'avg_spending', 'avg_transactions']
        df4.to_csv(f"{agg_folder}/avg_spending_vs_tx.csv", index=False)
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df4, x='avg_transactions', y='avg_spending')
        for i in range(len(df4)):
            plt.text(df4.avg_transactions[i] + 0.1, df4.avg_spending[i], df4.country[i])
        plt.title("Avg Transactions vs. Avg Spending by Country")
        plt.xlabel("Avg Transactions")
        plt.ylabel("Avg Spending ($)")
        plt.tight_layout()
        plt.savefig(f"{vis_folder}/avg_spending_vs_tx.png")
        plt.close()
    else:
        print("⚠️ No data returned for Avg Stats")
except Exception as e:
    print(f"❌ Avg Spending vs. Transactions plot failed: {e}")

print("✅ Aggregates and visualizations saved to results folders.")
