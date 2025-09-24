
# SportRadar Event Explorer

A complete end-to-end project for **managing, visualizing, and analyzing sports competition data** using the Sportradar Tennis API.

---

## 📌 Features

- **Data Extraction:** Fetch competitions, complexes, venues, and doubles competitor rankings from Sportradar API.
- **SQL Database:** Store structured information in normalized tables (Categories, Competitions, Complexes, Venues, Competitors, Competitor_Rankings).
- **Analysis Queries:** Predefined SQL queries for event exploration, trend analysis, and rankings.
- **Streamlit App:** Interactive web dashboard with search, filters, tables, and charts.

---

## 🛠️ Tech Stack

- **Language:** Python 3
- **Database:** MySQL (or PostgreSQL)
- **Frontend:** Streamlit
- **ORM/DB Access:** SQLAlchemy
- **API:** Sportradar Tennis API

---

## 🚀 Setup Instructions

1. **Clone Repository**
   ```bash
   git clone https://github.com/yourusername/sport-radar-event-explorer.git
   cd sport-radar-event-explorer
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Database Setup**
   - Create database: `CREATE DATABASE sportradar_db;`
   - Run schema file:
     ```bash
     mysql -u user -p sportradar_db < schema_and_queries.sql
     ```

4. **API & ETL Configuration**
   - Open `sportradar_api_etl.py`
   - Set your Sportradar API key: `SPORTRADAR_API_KEY`
   - Update `DB_CONNECTION_STRING` with DB credentials.
   - Run ETL script:
     ```bash
     python sportradar_api_etl.py
     ```

5. **Launch Streamlit App**
   ```bash
   streamlit run app.py
   ```

6. **Access Dashboard**
   - Open browser: `http://localhost:8501`
   - Navigate between dashboard pages (Competitions, Venues, Rankings, Country Analysis).

---

## 📊 Example Insights

- Top 5 doubles competitors by points
- Distribution of competitions by category
- Venues grouped by country
- Country-wise average points of competitors

---

## 📂 Project Deliverables

- `schema_and_queries.sql` → Database schema + required SQL queries
- `sportradar_api_etl.py` → API integration & ETL pipeline
- `app.py` → Streamlit app for analysis and visualization
- `README.md` → Documentation (this file)

---

## ✅ Evaluation Metrics

- **Data Extraction Accuracy**
- **SQL Database Design Quality**
- **Query Efficiency**
- **Streamlit App Functionality**
- **Documentation Quality**
- **Usability & Insights**

---

## ⚠️ Notes

- Sportradar APIs may require **trial or paid key**. Replace placeholder in scripts with your valid API key.
- Adjust DB connection string depending on whether you use **MySQL** or **PostgreSQL**.
- Some visualizations (maps, charts) are placeholders – extend with more interactive filters for bonus points.

