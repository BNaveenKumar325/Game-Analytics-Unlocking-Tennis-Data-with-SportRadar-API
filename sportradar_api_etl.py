
#!/usr/bin/env python3
"""sportradar_api_etl.py
ETL script to fetch data from Sportradar Tennis endpoints and populate a MySQL/Postgres database.
USAGE (high-level):
1. Fill in the CONFIG section with your SPORTRADAR_API_KEY and DB credentials.
2. Install requirements: pip install requests sqlalchemy pymysql python-dotenv
   (If using PostgreSQL, install psycopg2-binary and adjust connection string)
3. Run: python3 sportradar_api_etl.py
Note: This script contains robust error handling, rate-limit backoff, and transforms nested JSON into flat rows.
"""

import time
import logging
import sys
from typing import Dict, Any, List, Optional

import requests
from sqlalchemy import create_engine, text, Table, Column, MetaData, Integer, String, Date, ForeignKey
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# -------------------- CONFIG --------------------
# TODO: Replace placeholders before running
SPORTRADAR_API_KEY = 'YOUR_API_KEY_HERE'  # <- set this to your Sportradar API key
# Example endpoints (replace with exact paths and query params as required by Sportradar)
ENDPOINTS = {
    'competitions': 'https://api.sportradar.com/tennis/trial/v2/en/competitions.json?api_key={api_key}',
    'complexes': 'https://api.sportradar.com/tennis/trial/v2/en/complexes.json?api_key={api_key}',
    'doubles_rankings': 'https://api.sportradar.com/tennis/trial/v2/en/doubles_competitor_rankings.json?api_key={api_key}'
}

# Database connection - update to your DB user/password/host/dbname
# MySQL example: mysql+pymysql://user:password@host:3306/dbname
# PostgreSQL example: postgresql+psycopg2://user:password@host:5432/dbname
DB_CONNECTION_STRING = 'mysql+pymysql://db_user:db_password@localhost:3306/sportradar_db'

# ------------------ LOGGING ---------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ------------------ HELPERS ---------------------
def get(engine: Engine, url: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 5) -> dict:
    """Perform GET request with simple retry + backoff for rate-limits or transient errors."""
    for attempt in range(max_retries):
        try:
            logging.info('GET %s (attempt %d)', url, attempt + 1)
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (429, 503):  # rate limit or service unavailable
                wait = (attempt + 1) * 5
                logging.warning('Rate-limited or service unavailable (status %d). Backing off %ds', resp.status_code, wait)
                time.sleep(wait)
            else:
                logging.error('HTTP error %d: %s', resp.status_code, resp.text[:200])
                resp.raise_for_status()
        except requests.RequestException as e:
            logging.exception('Request failed: %s', e)
            time.sleep((attempt + 1) * 2)
    raise RuntimeError('Max retries exceeded for URL: %s' % url)


def upsert_table(engine: Engine, table_name: str, rows: List[Dict[str, Any]], pk: str):
    """Simple upsert helper for MySQL using REPLACE INTO semantics. For Postgres, adjust to INSERT ... ON CONFLICT."""
    if not rows:
        logging.info('No rows to upsert for %s', table_name)
        return
    conn = engine.connect()
    trans = conn.begin()
    try:
        # Build REPLACE INTO for MySQL
        keys = rows[0].keys()
        cols = ', '.join('`{}`'.format(k) for k in keys)
        vals_placeholder = ', '.join(':%s' % k for k in keys)
        # For portability we use SQLAlchemy text() with parameter binding per-row
        stmt = text('REPLACE INTO %s (%s) VALUES (%s)' % (table_name, cols, vals_placeholder))
        for row in rows:
            conn.execute(stmt, **row)
        trans.commit()
        logging.info('Upserted %d rows into %s', len(rows), table_name)
    except SQLAlchemyError as e:
        trans.rollback()
        logging.exception('DB upsert failed for %s: %s', table_name, e)
    finally:
        conn.close()


def normalize_competition(json_item: dict) -> Dict[str, Any]:
    """Map competition JSON to flat dict matching Competitions table."""
    return {
        'competition_id': json_item.get('id') or json_item.get('competition_id'),
        'competition_name': json_item.get('name') or json_item.get('competition_name') or '',
        'parent_id': json_item.get('parent_id'),
        'type': json_item.get('type') or json_item.get('competition_type') or '',
        'gender': json_item.get('gender') or json_item.get('gender_type') or '',
        'category_id': json_item.get('category_id') or json_item.get('category', {}).get('id')
    }


def normalize_category(json_item: dict) -> Dict[str, Any]:
    return {
        'category_id': json_item.get('id'),
        'category_name': json_item.get('name') or json_item.get('category_name') or ''
    }


def normalize_complex(json_item: dict) -> Dict[str, Any]:
    return {
        'complex_id': json_item.get('id'),
        'complex_name': json_item.get('name') or json_item.get('complex_name') or ''
    }


def normalize_venue(json_item: dict) -> Dict[str, Any]:
    return {
        'venue_id': json_item.get('id'),
        'venue_name': json_item.get('name') or json_item.get('venue_name') or '',
        'city_name': (json_item.get('city') or {}).get('name') or json_item.get('city_name') or '',
        'country_name': (json_item.get('country') or {}).get('name') or json_item.get('country_name') or '',
        'country_code': (json_item.get('country') or {}).get('code') or json_item.get('country_code') or 'UNK',
        'timezone': json_item.get('timezone') or json_item.get('tz') or '',
        'complex_id': json_item.get('complex_id') or (json_item.get('complex') or {}).get('id')
    }


def normalize_competitor(json_item: dict) -> Dict[str, Any]:
    return {
        'competitor_id': json_item.get('id'),
        'name': json_item.get('name') or json_item.get('full_name') or '',
        'country': (json_item.get('country') or {}).get('name') or json_item.get('country_name') or '',
        'country_code': (json_item.get('country') or {}).get('code') or json_item.get('country_code') or 'UNK',
        'abbreviation': json_item.get('abbreviation') or json_item.get('abbr') or (json_item.get('name') or '')[:10]
    }


def normalize_ranking(json_item: dict) -> Dict[str, Any]:
    # Example mapping; adjust keys to actual API response fields
    return {
        'rank_pos': json_item.get('rank'),
        'movement': json_item.get('movement') or 0,
        'points': json_item.get('points') or 0,
        'competitions_played': json_item.get('competitions_played') or json_item.get('competitions') or 0,
        'competitor_id': (json_item.get('competitor') or {}).get('id') or json_item.get('competitor_id'),
        'ranking_date': json_item.get('ranking_date') or None
    }

# ------------------ MAIN FLOW --------------------
def main():
    # Validate config
    if SPORTRADAR_API_KEY.startswith('YOUR_'):
        logging.error('Set SPORTRADAR_API_KEY in this file before running. Exiting.')
        sys.exit(1)
    if 'db_user' in DB_CONNECTION_STRING or 'db_password' in DB_CONNECTION_STRING:
        logging.error('Set DB_CONNECTION_STRING with real credentials. Exiting.')
        sys.exit(1)

    engine = create_engine(DB_CONNECTION_STRING, echo=False, pool_size=5, max_overflow=10)
    logging.info('DB engine created.')

    # 1) Competitions & Categories
    try:
        url = ENDPOINTS['competitions'].format(api_key=SPORTRADAR_API_KEY)
        data = get(engine, url)
        # The JSON structure may contain categories and competitions arrays; adapt as per actual response
        categories = data.get('categories') or data.get('category', [])
        competitions = data.get('competitions') or data.get('items') or []

        cat_rows = []
        comp_rows = []
        for cat in categories:
            cat_rows.append(normalize_category(cat))
            # Some APIs nest competitions under categories
            for comp in (cat.get('competitions') or []):
                comp_rows.append(normalize_competition(comp))

        # If competitions are at top-level
        for comp in competitions:
            comp_rows.append(normalize_competition(comp))

        # Upsert to DB
        upsert_table(engine, 'Categories', cat_rows, pk='category_id')
        upsert_table(engine, 'Competitions', comp_rows, pk='competition_id')
    except Exception as e:
        logging.exception('Failed to process competitions: %s', e)

    # 2) Complexes & Venues
    try:
        url = ENDPOINTS['complexes'].format(api_key=SPORTRADAR_API_KEY)
        data = get(engine, url)
        complexes = data.get('complexes') or data.get('items') or []
        complex_rows = []
        venue_rows = []
        for comp in complexes:
            complex_rows.append(normalize_complex(comp))
            for venue in comp.get('venues', []):
                venue_rows.append(normalize_venue(venue))

        upsert_table(engine, 'Complexes', complex_rows, pk='complex_id')
        upsert_table(engine, 'Venues', venue_rows, pk='venue_id')
    except Exception as e:
        logging.exception('Failed to process complexes: %s', e)

    # 3) Doubles competitor rankings & competitors
    try:
        url = ENDPOINTS['doubles_rankings'].format(api_key=SPORTRADAR_API_KEY)
        data = get(engine, url)
        rankings = data.get('rankings') or data.get('items') or []

        competitor_rows = []
        ranking_rows = []
        for r in rankings:
            # r may contain competitor object and ranking fields
            competitor = (r.get('competitor') or {})
            if competitor:
                competitor_rows.append(normalize_competitor(competitor))
            ranking_rows.append(normalize_ranking(r))

        upsert_table(engine, 'Competitors', competitor_rows, pk='competitor_id')
        upsert_table(engine, 'Competitor_Rankings', ranking_rows, pk='rank_id')
    except Exception as e:
        logging.exception('Failed to process doubles rankings: %s', e)

    logging.info('ETL completed.')

if __name__ == '__main__':
    main()
