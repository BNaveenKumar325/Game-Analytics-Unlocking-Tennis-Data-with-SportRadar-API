
#!/usr/bin/env python3
# app.py - Streamlit application for SportRadar Event Explorer

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# ---------------- CONFIG ----------------
# Adjust this to match your DB connection (same as in ETL)
DB_CONNECTION_STRING = 'mysql+pymysql://db_user:db_password@localhost:3306/sportradar_db'

# ---------------- HELPER ----------------
@st.cache_data
def run_query(query, params=None):
    engine = create_engine(DB_CONNECTION_STRING)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)

# ---------------- LAYOUT ----------------
st.set_page_config(page_title="SportRadar Event Explorer", layout="wide")

st.title("🏆 SportRadar Event Explorer")
st.markdown("Explore, analyze, and visualize competition and competitor data from Sportradar API.")

# Sidebar Navigation
pages = [
    "Dashboard",
    "Competitions",
    "Venues & Complexes",
    "Competitor Rankings",
    "Country Analysis"
]
choice = st.sidebar.radio("Navigate", pages)

# ---------------- PAGES -----------------
if choice == "Dashboard":
    st.header("📊 Dashboard Overview")
    total_competitors = run_query("SELECT COUNT(*) AS total FROM Competitors")["total"].iloc[0]
    total_countries = run_query("SELECT COUNT(DISTINCT country) AS c FROM Competitors")["c"].iloc[0]
    highest_points = run_query("SELECT MAX(points) AS max_points FROM Competitor_Rankings")["max_points"].iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Competitors", total_competitors)
    c2.metric("Countries Represented", total_countries)
    c3.metric("Highest Points", highest_points)

elif choice == "Competitions":
    st.header("🎾 Competitions")
    df = run_query("""
        SELECT c.competition_id, c.competition_name, c.type, c.gender, cat.category_name
        FROM Competitions c LEFT JOIN Categories cat ON c.category_id = cat.category_id
    """)
    st.dataframe(df, use_container_width=True)

    st.subheader("Competitions by Category")
    chart_df = run_query("""
        SELECT cat.category_name, COUNT(*) AS competition_count
        FROM Competitions c LEFT JOIN Categories cat ON c.category_id = cat.category_id
        GROUP BY cat.category_name
    """)
    st.bar_chart(chart_df.set_index("category_name"))

elif choice == "Venues & Complexes":
    st.header("🏟️ Venues and Complexes")
    df = run_query("""
        SELECT v.venue_id, v.venue_name, v.city_name, v.country_name, comp.complex_name, v.timezone
        FROM Venues v LEFT JOIN Complexes comp ON v.complex_id = comp.complex_id
    """)
    st.dataframe(df, use_container_width=True)

    st.subheader("Venues by Country")
    chart_df = run_query("""
        SELECT country_name, COUNT(*) AS venues FROM Venues GROUP BY country_name
    """)
    st.bar_chart(chart_df.set_index("country_name"))

elif choice == "Competitor Rankings":
    st.header("🏅 Competitor Rankings")
    df = run_query("""
        SELECT comp.name, comp.country, r.rank_pos, r.points, r.movement, r.competitions_played
        FROM Competitors comp JOIN Competitor_Rankings r ON comp.competitor_id = r.competitor_id
        ORDER BY r.rank_pos LIMIT 50
    """)
    st.dataframe(df, use_container_width=True)

    st.subheader("Top 5 Competitors")
    df_top5 = run_query("""
        SELECT comp.name, r.rank_pos, r.points
        FROM Competitors comp JOIN Competitor_Rankings r ON comp.competitor_id = r.competitor_id
        WHERE r.rank_pos <= 5 ORDER BY r.rank_pos
    """)
    st.table(df_top5)

elif choice == "Country Analysis":
    st.header("🌍 Country-Wise Analysis")
    df = run_query("""
        SELECT comp.country, COUNT(*) AS competitor_count, AVG(r.points) AS avg_points
        FROM Competitors comp
        JOIN Competitor_Rankings r ON comp.competitor_id = r.competitor_id
        GROUP BY comp.country ORDER BY competitor_count DESC
    """)
    st.dataframe(df, use_container_width=True)
    st.map(df.rename(columns={"country": "name"}), latitude="competitor_count", longitude="avg_points")  # placeholder map usage

st.sidebar.markdown("---")
st.sidebar.info("Project: SportRadar Event Explorer | Built with Streamlit + SQLAlchemy")
