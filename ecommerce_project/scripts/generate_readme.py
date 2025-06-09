import os
from datetime import datetime

def generate_readme():
    # Define the README content
    readme_content = """# E-Commerce Big Data Analytics Project

## Overview
This repository contains a big data analytics pipeline for an e-commerce dataset, utilizing MongoDB, HBase, Apache Spark, and Python visualizations. The project fulfills a university assignment to model, process, analyze, and visualize e-commerce data, covering user profiles, products, categories, sessions, and transactions. It demonstrates data modeling, distributed processing, integrated analytics, and insight visualization, with a focus on scalability and actionable business outcomes.

## Repository Structure
```
ecommerce-analytics/
├── data/
│   ├── users.json                # User profiles
│   ├── products.json             # Product details
│   ├── categories.json           # Category hierarchies
│   ├── transactions.json         # Purchase records
│   ├── sessions_0.json           # Session data (chunk 1)
│   ├── sessions_1.json           # Session data (chunk 2)
│   └── ...                       # Additional session chunks
├── scripts/
│   ├── data_generator.py         # Generates dataset
│   ├── mongodb_setup.py          # MongoDB schema and queries
│   ├── hbase_setup.py            # HBase schema and queries
│   ├── spark_processing.py       # PySpark data cleaning and recommendations
│   ├── visualizations.py         # Matplotlib/Seaborn visualizations
│   └── data_integration.py       # Integrates MongoDB, HBase, Spark for CLV
├── results/
│   ├── co_purchase_recommendations.csv  # Co-purchase analysis
│   ├── clv_results.csv           # Customer Lifetime Value results
│   ├── sales_trend.png           # Sales trend plot
│   ├── top_products.png          # Top products plot
│   └── user_segmentation.png     # User segmentation plot
├── docs/
│   ├── technical_report.tex      # LaTeX source for technical report
│   ├── technical_report.pdf      # Compiled report
│   └── references.bib            # Bibliography for report
├── .gitignore                    # Git ignore file
├── README.md                     # This file
├── requirements.txt              # Python dependencies
└── docker-compose.yml            # Docker setup for MongoDB and HBase
```

## Setup Instructions
1. **Prerequisites**:
   - Python 3.8+
   - MongoDB (local or Docker)
   - HBase (Docker with Thrift server)
   - Apache Spark (local or cluster)
   - LaTeX distribution (e.g., TeX Live, MiKTeX)
   - Docker and Docker Compose (optional, for simplified setup)
   - Java 8 or 11 (for Spark)

2. **Clone Repository**:
   ```bash
   git clone <repository-url>
   cd ecommerce-analytics
   ```

3. **Set Up Virtual Environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # macOS/Linux
   .venv\\Scripts\\activate     # Windows
   ```

4. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

5. **Start Databases**:
   - Using Docker Compose:
     ```bash
     docker-compose up -d
     ```
   - Or manually:
     - MongoDB: `docker run -d -p 27017:27017 mongo`
     - HBase: `docker run -d -p 2181:2181 -p 16000:16000 -p 16010:16010 -p 9090:9090 harisekhon/hbase`

6. **Generate Dataset**:
   ```bash
   python scripts/data_generator.py
   ```
   For testing, edit `data_generator.py` to reduce sizes (e.g., `NUM_USERS=1000`, `NUM_SESSIONS=20000`).

7. **Load Data**:
   - MongoDB:
     ```bash
     python scripts/mongodb_setup.py
     ```
   - HBase:
     ```bash
     python scripts/hbase_setup.py
     ```

8. **Run Analytics**:
   - Spark processing (co-purchase recommendations):
     ```bash
     spark-submit scripts/spark_processing.py
     ```
   - Integrated analytics (CLV):
     ```bash
     python scripts/data_integration.py
     ```
   - Visualizations:
     ```bash
     python scripts/visualizations.py
     ```

9. **Compile Technical Report**:
   ```bash
   cd docs
   latexmk -pdf technical_report.tex
   ```

10. **Prepare for Submission**:
    - Include a small data sample (e.g., 1,000 users) in `data/`.
    - Push to GitHub:
      ```bash
      git add .
      git commit -m "Final project submission"
      git push origin main
      ```

## Usage
1. Run `data_generator.py` to create JSON files in `data/`.
2. Load data into databases using `mongodb_setup.py` and `hbase_setup.py`.
3. Execute `spark_processing.py` for co-purchase recommendations, saved to `results/co_purchase_recommendations.csv`.
4. Run `data_integration.py` for Customer Lifetime Value analysis, saved to `results/clv_results.csv`.
5. Generate plots with `visualizations.py`, saved as `results/sales_trend.png`, `results/top_products.png`, and `results/user_segmentation.png`.
6. Compile `technical_report.tex` to produce `technical_report.pdf` with embedded visualizations and analysis details.
7. Review results in `results/` and the report in `docs/`.

## Notes
- **Dataset Size**: The full dataset includes 2M sessions (~20 JSON files, 100,000 sessions each), 500,000 transactions, 10,000 users, 5,000 products, and 25 categories. Ensure sufficient disk space (~10GB+).
- **Subcategory_id**: The `products.json` omits `subcategory_id` (despite the assignment schema) but uses `category_id`, sufficient for analytics. Modify `data_generator.py` if subcategories are needed.
- **Scalability**: The pipeline supports MongoDB sharding, HBase region splitting, and Spark clustering, detailed in the technical report.
- **HBase Configuration**: Requires ZooKeeper and Thrift server (port 9090). Verify connectivity before running `hbase_setup.py`.
- **Visualizations**: Static plots use Matplotlib/Seaborn. Extend to Plotly for interactivity if desired.
- **Items Structure**: Transaction `items` include `product_id`, `quantity`, `unit_price`, and `subtotal`, used in analytics scripts.
- **Testing**: Reduce dataset sizes in `data_generator.py` for local testing to avoid memory issues.
- **Bonus**: An Insyt.co dashboard (10% bonus) is not included but can be added with a supported database.

## License
MIT License
"""

    # Write to README.md in the project root
    readme_path = os.path.join("..", "README.md")  # Assumes script is in scripts/
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(readme_content)
    
    print(f"Generated README.md at {readme_path}")

if __name__ == "__main__":
    # Ensure the script is run from scripts/ directory
    if not os.path.basename(os.getcwd()) == "scripts":
        print("Please run this script from the scripts/ directory")
        exit(1)
    
    generate_readme()