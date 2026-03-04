import streamlit as st
import pandas as pd
import requests
import json
import pipeline
import os
from pipeline import engine

import streamlit as st

from dotenv import load_dotenv


# Load environment variables (optional if already done in pipeline)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Test database connection
if engine:
    try:
        with engine.connect() as conn:
            st.success("Database connected successfully ✅")
    except Exception as e:
        st.error(f"Connection failed: {e}")
else:
    st.error("Engine is None")

# -------- API --------
API_KEY = os.getenv("API_KEY") or "DUMMY_KEY"

headers = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}


# =============================
# RESET ERROR STATE
# =============================

if "has_error" not in st.session_state:
    st.session_state.has_error = False

# Reset on each app run
st.session_state.has_error = False

# ---------- Page Config ----------
st.set_page_config(
    page_title="Cricket Analytics Dashboard",
    page_icon="🏏",
    layout="wide"
)

# ---------- Styling ----------
st.markdown("""
<style>
/* Main background */
section[data-testid="stAppViewContainer"] {
    background-color: #f4f6f9;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background-color: #eaf0f6;
}

/* Tab styling */
.stTabs [data-baseweb="tab-list"] {
    gap: 24px;
}

.stTabs [data-baseweb="tab"] {
    height: 50px;
    white-space: pre-wrap;
    background-color: #ffffff;
    border-radius: 4px 4px 0px 0px;
    gap: 1px;
    padding-top: 10px;
    padding-bottom: 10px;
}

.stTabs [aria-selected="true"] {
    background-color: #eaf0f6;
}

</style>
""", unsafe_allow_html=True)

# Helper: Transform API values to Dataframe records
def transform_to_records(data):
    if not data or "values" not in data:
        return []
    h = data.get("headers", [])
    v = data.get("values", [])
    return [dict(zip(h, row.get("values", []))) for row in v if len(row.get("values", [])) == len(h)]

@st.cache_data(ttl=3600)
def search_player(name):
    url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/search"
    response = requests.get(url, headers=headers, params={"plrN": name})
    return response.json().get("player", []) if response.status_code == 200 else None

@st.cache_data(ttl=3600)
def get_player_stats(player_id):
    # Fetch batting and bowling separately as the main profile lacks the "stats" key
    bat_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/batting"
    bowl_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/bowling"
    
    bat_resp = requests.get(bat_url, headers=headers)
    bowl_resp = requests.get(bowl_url, headers=headers)
    
    combined_stats = []
    if bat_resp.status_code == 200:
        combined_stats.append({"type": "Batting Statistics", "values": transform_to_records(bat_resp.json())})
    if bowl_resp.status_code == 200:
        combined_stats.append({"type": "Bowling Statistics", "values": transform_to_records(bowl_resp.json())})
        
    return {"stats": combined_stats} if combined_stats else None


# Initialize Error Tracking
if 'has_error' not in st.session_state:
    st.session_state.has_error = False

# ---------- Sidebar ----------
st.sidebar.title("🏏 Cricket Dashboard")

# Icons similar to common sports apps
LIVE = "📋 Matches Dashboard"
PLAYER = "👤 Player Statistics"
SQL = "📊 SQL Analytics"
CRUD = "🔧 CRUD Operations"

# Dynamic sidebar labels with error indicator
sidebar_options = [LIVE, PLAYER, SQL, CRUD]
if st.session_state.has_error:
    # Append error icon to current selected option or global warning
    st.sidebar.warning("⚠️ Some data failed to load")

option = st.sidebar.radio(
    "Navigation Menu",
    sidebar_options,
    index=0
)


# ---------- Navigation ----------
@st.cache_data(ttl=60)
def get_match_full_details(match_id):
    try:
        # Match Info
        info_url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}"
        info_resp = requests.get(info_url, headers=headers)
        
        # Scorecard
        sc_url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}/scard"
        sc_resp = requests.get(sc_url, headers=headers)
        
        if info_resp.status_code != 200:
            return None, f"Match Info API failed with status {info_resp.status_code}"
        if sc_resp.status_code != 200:
            return None, f"Scorecard API failed with status {sc_resp.status_code}"
            
        return {"info": info_resp.json(), "scard": sc_resp.json()}, None
    except Exception as e:
        return None, str(e)


# ---------- Navigation ----------
if option == LIVE:
    st.header("📋 Matches Dashboard")
    st.caption("Real-time match insights from Cricbuzz")

    # Fetch data once to populate dropdown
    df_live = pipeline.live_data()
    df_recent = pipeline.recent_match_data()
    df_upcoming = pipeline.upcoming_data()

    # Dropdown for scorecard selection
    match_options = {}
    if not df_live.empty:
        for _, row in df_live.iterrows():
            match_options[f"🔴 LIVE: {row['Team 1']} vs {row['Team 2']} ({row['Match ID']})"] = row['Match ID']
    if not df_recent.empty:
        for _, row in df_recent.iterrows():
            match_options[f"📅 RECENT: {row['Team 1']} vs {row['Team 2']} ({row['Match ID']})"] = row['Match ID']
    if not df_upcoming.empty:
        for _, row in df_upcoming.iterrows():
            match_options[f"⏳ UPCOMING: {row['Team 1']} vs {row['Team 2']} ({row['Match ID']})"] = row['Match ID']

    st.divider()
    c1, c2 = st.columns([3, 1])
    selected_match_label = c1.selectbox("Select match for detailed scorecard", list(match_options.keys()))
    load_sc = c2.button("Load Full Scorecard", type="primary", use_container_width=True)

    if load_sc and selected_match_label:
        mid = match_options[selected_match_label]
        with st.spinner("Fetching full scorecard..."):
            data, err = get_match_full_details(mid)
            if err:
                st.error(f"Failed to load scorecard: {err}")
            elif not data:
                st.warning("No data found for this match.")
            else:
                info = data["info"]
                scard = data["scard"]
                
                # --- MATCH INFO ---
                st.markdown("---")
                st.subheader(f"🏟 {info.get('matchdesc', 'Match Details')}")
                i_col1, i_col2 = st.columns(2)
                i_col1.write(f"**Teams:** {info.get('team1', {}).get('teamName')} vs {info.get('team2', {}).get('teamName')}")
                i_col1.write(f"**Status:** {info.get('status')}")
                i_col2.write(f"**Venue:** {info.get('venueinfo', {}).get('ground')}, {info.get('venueinfo', {}).get('city')}")
                if info.get('tossstatus'):
                    i_col2.write(f"**Toss:** {info.get('tossstatus')}")

                # --- SCORECARD LOOP ---
                if "scorecard" in scard and scard["scorecard"]:
                    for innings in scard["scorecard"]:
                        bat_team = innings.get('batTeamName', 'Innings')
                        st.markdown(f"### 🏏 {bat_team} Scorecard")
                        
                        # Innings Summary
                        s_runs = innings.get('score', 0)
                        s_wick = innings.get('wickets', 0)
                        s_overs = innings.get('overs', 0)
                        rr = innings.get('runRate', 'N/A')
                        
                        m1, m2, m3 = st.columns(3)
                        m1.metric("Score", f"{s_runs}/{s_wick}")
                        m2.metric("Overs", f"{s_overs}")
                        m3.metric("Run Rate", f"{rr}")
                        
                        # Target and RRR for 2nd innings or later
                        if innings.get('inningsId', 1) > 1:
                            target = innings.get('target')
                            rrr = innings.get('requiredRunRate')
                            if target or rrr:
                                c_rrr1, c_rrr2 = st.columns(2)
                                if target: c_rrr1.write(f"**Target:** {target}")
                                if rrr: c_rrr2.write(f"**Required RR:** {rrr}")
                        
                        # Batting
                        st.write("#### Batting")
                        bat_list = []
                        # BatsmenData is a dict mapping player string IDs to info
                        for _, b_info in innings.get("batsmenData", {}).items():
                            bat_list.append({
                                "Batter": b_info.get("name"),
                                "Runs": b_info.get("runs"),
                                "Balls": b_info.get("balls"),
                                "4s": b_info.get("fours"),
                                "6s": b_info.get("sixes"),
                                "SR": b_info.get("strikeRate"),
                                "Dismissal": b_info.get("outDesc")
                            })
                        if bat_list:
                            st.dataframe(pd.DataFrame(bat_list), use_container_width=True, hide_index=True)
                        
                        # Bowling
                        st.write("#### Bowling")
                        bowl_list = []
                        for _, bw_info in innings.get("bowlersData", {}).items():
                            bowl_list.append({
                                "Bowler": bw_info.get("name"),
                                "O": bw_info.get("overs"),
                                "M": bw_info.get("maidens"),
                                "R": bw_info.get("runs"),
                                "W": bw_info.get("wickets"),
                                "Eco": bw_info.get("economy")
                            })
                        if bowl_list:
                            st.dataframe(pd.DataFrame(bowl_list), use_container_width=True, hide_index=True)
                else:
                    st.info("Detailed scorecard not yet available for this match.")

    st.markdown("---")
    tab_live, tab_recent, tab_upcoming = st.tabs(["🔴 Live matches", "📅 Recent Matches", "⏳ Upcoming Matches"])

    with tab_live:
        st.subheader("Live Matches Summary")
        if not df_live.empty:
            cols = ["Team 1", "Team 2", "Team1 Runs", "Team1 Wickets", "Team2 Runs", "Team2 Wickets", "Status", "Venue", "Format"]
            available_cols = [c for c in cols if c in df_live.columns]
            st.dataframe(df_live[available_cols], use_container_width=True)
        else:
            st.info("No live matches at the moment.")

    with tab_recent:
        st.subheader("Recent Matches Summary")
        if not df_recent.empty:
            cols = ["Team 1", "Team 2", "Status", "Venue", "Match Format", "Start Date"]
            available_cols = [c for c in cols if c in df_recent.columns]
            st.dataframe(df_recent[available_cols], use_container_width=True)
        else:
            st.info("No recent matches found.")

    with tab_upcoming:
        st.subheader("Upcoming Matches Summary")
        if not df_upcoming.empty:
            cols = ["Team 1", "Team 2", "Status", "Venue", "Format", "Start Date"]
            available_cols = [c for c in cols if c in df_upcoming.columns]
            st.dataframe(df_upcoming[available_cols], use_container_width=True)
        else:
            st.info("No upcoming matches scheduled.")


elif option == PLAYER:
    st.title("Player Statistics")

    # Initialize session state for player search
    if 'p_search_results' not in st.session_state:
        st.session_state.p_search_results = None

    player_name = st.text_input("Enter Player Name")

    if st.button("Get Player Stats"):
        with st.spinner("Searching for player..."):
            results = search_player(player_name)
            st.session_state.p_search_results = results
            if not results:
                st.error("Player not found")

    if st.session_state.p_search_results:
        players = st.session_state.p_search_results
        player_options = {f"{p['name']} ({p.get('teamName', 'India')})": p["id"] for p in players}

        selected_player = st.selectbox(
            "Select Player",
            list(player_options.keys())
        )

        player_id = player_options[selected_player]

        with st.spinner("Fetching performance data..."):
            stats_data = get_player_stats(player_id)

        if not stats_data:
            st.error("Stats not available for this player")
        else:
            # ONLY DISPLAY STATS SECTION (User snippet logic)
            for section in stats_data.get("stats", []):
                stat_type = section.get("type", "").title()
                st.subheader(stat_type)
                df = pd.DataFrame(section.get("values", []))
                if df.empty:
                    st.info("No data available")
                else:
                    st.dataframe(df, use_container_width=True, hide_index=True)



elif option == SQL:
    st.header("📊 Analytics Questions")
    st.caption("25 Analytical queries powered by pipeline.py")

    questions = [f"Question {i}" for i in range(1, 26)]
    question_labels = {
        "Question 1": "All players who represent India",
        "Question 2": "Matches played in the last few days",
        "Question 3": "Top 10 highest run scorers in ODI",
        "Question 4": "Venues with capacity exceeding 25k",
        "Question 5": "Matches won by each team",
        "Question 6": "Player count by playing role",
        "Question 7": "Highest individual score per format",
        "Question 8": "Series started in 2024",
        "Question 9": "All-rounders with 1000 runs & 50 wickets",
        "Question 10": "Last 20 completed matches details",
        "Question 11": "Player performance comparison across formats",
        "Question 12": "Team performance Home vs Away",
        "Question 13": "Batting partnerships of 100+ runs",
        "Question 14": "Bowling performance at different venues",
        "Question 15": "Players in close matches",
        "Question 16": "Yearly batting performance evolution",
        "Question 17": "Toss advantage percentage analysis",
        "Question 18": "Most economical bowlers in limited overs",
        "Question 19": "Batsmen scoring consistency analysis",
        "Question 20": "Player format analysis & batting averages",
        "Question 21": "Composite player ranking system",
        "Question 22": "Head-to-head match prediction",
        "Question 23": "Recent player form categorized",
        "Question 24": "Successful batting partnerships analysis",
        "Question 25": "Time-series analysis of player evolution"
    }

    selected_q = st.selectbox("Select a query", questions, format_func=lambda x: f"{x}: {question_labels.get(x, '')}")
        
    if st.button("Execute"):
        
        with st.spinner("Calculating..."):
            try:
                res = pd.DataFrame()
                if selected_q == "Question 1":res = pipeline.get_q1_india_players()
                elif selected_q == "Question 2":res = pipeline.get_q2_recent_matches()
                elif selected_q == "Question 3":res = pipeline.get_q3_top_odi_scorers()
                elif selected_q == "Question 4": res = pipeline.get_q4_large_venues()
                elif selected_q == "Question 5":res = pipeline.get_q5_team_win_counts()
                elif selected_q == "Question 6":res = pipeline.get_q6_players_by_role()
                elif selected_q == "Question 7": res = pipeline.get_q7_highest_scores()
                elif selected_q == "Question 8": res = pipeline.get_q8_series_2024()
                elif selected_q == "Question 9": res = pipeline.get_q9_allrounders()
                elif selected_q == "Question 10": res = pipeline.get_q10_last_20_completed_matches()
                elif selected_q == "Question 11": res = pipeline.get_q11_player_format_comparison()
                elif selected_q == "Question 12": 
                    st.subheader("ICC Cricket World Cup 2023 — Home vs Away Performance")
                    res = pipeline.get_que12_home_away_analysis()
                elif selected_q == "Question 13": res = pipeline.get_que13_century_partnerships()
                elif selected_q == "Question 14":
                    res, series_name = pipeline.get_que14_bowler_venue_performance()
                    st.subheader(f"Series: {series_name}")
                    if res.empty:
                      st.info(
                         "No bowlers satisfy the criteria:\n"
                         "• At least 3 matches at the same venue\n"
                         "• Minimum 4 overs in each match"
                         )
                    else:
                         st.dataframe(res)
                elif selected_q == "Question 15": res = pipeline.get_que15_close_matches_performance()
                elif selected_q == "Question 16":res = pipeline.get_que16_player_yearly_stats()
                elif selected_q == "Question 17": 
                    res = pipeline.get_q17_toss_advantage()
                    if isinstance(res, pd.DataFrame) and not res.empty:
                        st.subheader("Question 17 — Toss Advantage Analysis")
                        st.dataframe(res, use_container_width=True)
                    else:
                        st.warning("No data available for Question 17.")
                elif selected_q == "Question 18": res = pipeline.get_que18_economical_bowlers()
                elif selected_q == "Question 19":
                    try:
                        res = pipeline.get_que19_player_consistency()
                    except Exception as e:
                        st.error(f"Error: {e}")
                        res = pd.DataFrame()
                elif selected_q == "Question 20": res = pipeline.get_que20_player_format_analysis()
                elif selected_q == "Question 21":res = pipeline.get_q21_composite_ranking()
                elif selected_q == "Question 22":
                    st.subheader("🏏 Q22: India vs Australia Head-to-Head Analysis (2024–2026)")
                    st.caption("Data extracted from Cricbuzz series archives | Only completed matches included")
                    res = pipeline.get_q22_head_to_head_analysis()
                elif selected_q == "Question 23":res = pipeline.get_q23_player_form_analysis()
                elif selected_q == "Question 24":res = pipeline.get_q24_batting_partnerships()
                elif selected_q == "Question 25":res = pipeline.get_q25_player_time_series()
                else: res = pd.DataFrame() # Fallback 
                # -------- SAFE DISPLAY --------
                if isinstance(res, pd.DataFrame):

                  if not res.empty:
                      st.dataframe(res, use_container_width=True)
               
                  else:
                      st.warning("No data available for this query.")
                elif isinstance(res, dict):
                 # temporary fallback if any function still returns dict
                      st.write(res)
                elif res is None:
                      pass   # do nothing (custom UI already shown)

                else:
                      st.warning("No data available for this query.")

               
            except Exception as e:
                st.session_state.has_error = True
                st.error(f"Analysis failed: {e}")



elif option == CRUD:
    st.header("🛠 Database CRUD Operations")
    st.caption("Persistent storage powered by PostgreSQL")

    # ── Imports ──────────────────────────────────────────────────────────────
    try:
        import psycopg2
        from psycopg2 import sql as pg_sql
    except ImportError:
        st.error("❌ psycopg2 is not installed. Run: pip install psycopg2-binary")
        st.stop()

    # ── Credentials from environment variables ────────────────────────────────
    DB_HOST     = os.getenv("DB_HOST", "localhost")
    DB_PORT     = os.getenv("DB_PORT", "5432")
    DB_NAME     = os.getenv("DB_NAME", "cricbuzz")
    DB_USER     = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    missing_vars = []
    for var_name, var_val in [("DB_HOST", DB_HOST), ("DB_NAME", DB_NAME),
                               ("DB_USER", DB_USER), ("DB_PASSWORD", DB_PASSWORD)]:
        if not var_val:
            missing_vars.append(var_name)
    if missing_vars:
        st.warning(f"⚠️ Missing environment variables: {', '.join(missing_vars)}. "
                   "Set them in your .env file or system environment. "
                   "Using defaults where possible (DB_HOST=localhost, DB_PORT=5432, DB_NAME=cricbuzz).")

    # ── DB helpers ────────────────────────────────────────────────────────────
    def get_connection():
        """Return a new psycopg2 connection."""
        return psycopg2.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

    DEFAULT_PLAYERS = [
        ("Virat Kohli",       "India",        "Batsman",      275, 12898,   4),
        ("Rohit Sharma",      "India",        "Batsman",      243,  9837,   8),
        ("Jasprit Bumrah",    "India",        "Bowler",        72,   120, 121),
        ("Steve Smith",       "Australia",    "Batsman",      142,  4917,  28),
        ("Pat Cummins",       "Australia",    "Bowler",        75,   340, 124),
        ("Joe Root",          "England",      "Batsman",      158,  6207,  26),
        ("Kane Williamson",   "New Zealand",  "Batsman",      161,  6554,  37),
        ("Babar Azam",        "Pakistan",     "Batsman",      105,  5089,   0),
        ("Rashid Khan",       "Afghanistan",  "Bowler",        94,  1200, 172),
        ("Quinton de Kock",   "South Africa", "Wicketkeeper", 145,  5966,   0),
    ]

    def init_db(conn):
        """Create table if not exists; seed with 10 players if empty."""
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crud_players (
                    id       SERIAL PRIMARY KEY,
                    name     VARCHAR(100),
                    country  VARCHAR(100),
                    role     VARCHAR(50),
                    matches  INTEGER,
                    runs     INTEGER,
                    wickets  INTEGER
                );
            """)
            cur.execute("SELECT COUNT(*) FROM crud_players;")
            count = cur.fetchone()[0]
            if count == 0:
                cur.executemany(
                    "INSERT INTO crud_players (name, country, role, matches, runs, wickets) "
                    "VALUES (%s, %s, %s, %s, %s, %s);",
                    DEFAULT_PLAYERS
                )
        conn.commit()

    def fetch_all(conn, search=""):
        """Return all players (optionally filtered) as a DataFrame."""
        with conn.cursor() as cur:
            if search:
                cur.execute(
                    "SELECT id, name, country, role, matches, runs, wickets "
                    "FROM crud_players WHERE name ILIKE %s OR country ILIKE %s ORDER BY id;",
                    (f"%{search}%", f"%{search}%")
                )
            else:
                cur.execute(
                    "SELECT id, name, country, role, matches, runs, wickets "
                    "FROM crud_players ORDER BY id;"
                )
            rows = cur.fetchall()
        cols = ["ID", "Name", "Country", "Role", "Matches", "Runs", "Wickets"]
        return pd.DataFrame(rows, columns=cols)

    def add_player(conn, name, country, role, matches, runs, wickets):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO crud_players (name, country, role, matches, runs, wickets) "
                "VALUES (%s, %s, %s, %s, %s, %s);",
                (name, country, role, matches, runs, wickets)
            )
        conn.commit()

    def update_player(conn, pid, name, country, role, matches, runs, wickets):
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE crud_players SET name=%s, country=%s, role=%s, "
                "matches=%s, runs=%s, wickets=%s WHERE id=%s;",
                (name, country, role, matches, runs, wickets, pid)
            )
        conn.commit()
        return cur.rowcount  # 0 if ID not found

    def delete_player(conn, pid):
        with conn.cursor() as cur:
            cur.execute("DELETE FROM crud_players WHERE id=%s;", (pid,))
        conn.commit()
        return cur.rowcount  # 0 if ID not found

    # ── Connect & initialise ──────────────────────────────────────────────────
    try:
        _conn = get_connection()
        init_db(_conn)
    except Exception as _db_err:
        st.error(f"❌ Could not connect to PostgreSQL: {_db_err}\n\n"
                 "**Checklist:**\n"
                 "- PostgreSQL service is running\n"
                 "- Database `cricbuzz` exists (or set DB_NAME env var)\n"
                 "- Credentials in .env are correct (DB_USER / DB_PASSWORD)\n"
                 "- psycopg2-binary is installed")
        st.stop()

    # ── Fetch players for dropdowns ───────────────────────────────────────────
    try:
        players_df = fetch_all(_conn)
    except Exception as e:
        st.error(f"❌ Failed to fetch players: {e}")
        st.stop()

    # ── Full-width table at top ───────────────────────────────────────────────
    st.subheader("📋 Players Table (PostgreSQL)")
    search_q = st.text_input("🔍 Search by name or country...", key="crud_search")
    col_ref, col_count = st.columns([1, 5])
    with col_ref:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()
    with col_count:
        st.metric("Total Records", len(players_df))

    filtered_df = fetch_all(_conn, search=search_q.strip())
    if filtered_df.empty:
        st.info("No players found.")
    else:
        st.dataframe(filtered_df, use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Build dropdown options from live DB data ───────────────────────────────
    player_options = {
        f"{row['Name']} ({row['Country']}) — ID {row['ID']}": row
        for _, row in players_df.iterrows()
    }
    player_labels = list(player_options.keys())

    # ── Tabs for CRUD operations ───────────────────────────────────────────────
    tab_add, tab_update, tab_delete = st.tabs(["➕  Add Player", "✏️  Update Player", "🗑️  Delete Player"])

    # ════════════════════════ ADD ════════════════════════════════════════════
    with tab_add:
        st.markdown("##### Fill in details to add a new player")
        with st.form("add_player_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                a_name    = st.text_input("Player Name", placeholder="e.g. MS Dhoni")
                a_country = st.text_input("Country",     placeholder="e.g. India")
                a_role    = st.selectbox("Role", ["Batsman", "Bowler", "All-rounder", "Wicketkeeper"])
            with col2:
                a_matches = st.number_input("Matches", min_value=0, step=1)
                a_runs    = st.number_input("Runs",    min_value=0, step=1)
                a_wickets = st.number_input("Wickets", min_value=0, step=1)
            submitted_add = st.form_submit_button("➕ Add Player", use_container_width=True, type="primary")

        if submitted_add:
            if not a_name.strip():
                st.error("❌ Player name cannot be empty.")
            else:
                try:
                    add_player(_conn, a_name.strip(), a_country.strip(), a_role,
                               int(a_matches), int(a_runs), int(a_wickets))
                    st.success(f"✅ **{a_name}** added to the database!")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Failed to add player: {e}")

    # ════════════════════════ UPDATE ═════════════════════════════════════════
    with tab_update:
        st.markdown("##### Select a player from the dropdown — fields auto-fill")

        if not player_labels:
            st.info("No players in the database yet.")
        else:
            selected_label = st.selectbox("Select Player to Update", player_labels, key="upd_select")
            sel_row = player_options[selected_label]

            role_options = ["Batsman", "Bowler", "All-rounder", "Wicketkeeper"]
            current_role_idx = role_options.index(sel_row["Role"]) if sel_row["Role"] in role_options else 0

            with st.form("update_player_form", clear_on_submit=False):
                col1, col2 = st.columns(2)
                with col1:
                    u_name    = st.text_input("Name",    value=sel_row["Name"])
                    u_country = st.text_input("Country", value=sel_row["Country"])
                    u_role    = st.selectbox("Role", role_options, index=current_role_idx)
                with col2:
                    u_matches = st.number_input("Matches", min_value=0, step=1, value=int(sel_row["Matches"]))
                    u_runs    = st.number_input("Runs",    min_value=0, step=1, value=int(sel_row["Runs"]))
                    u_wickets = st.number_input("Wickets", min_value=0, step=1, value=int(sel_row["Wickets"]))
                submitted_upd = st.form_submit_button("💾 Save Changes", use_container_width=True, type="primary")

            if submitted_upd:
                if not u_name.strip():
                    st.error("❌ Name cannot be empty.")
                else:
                    try:
                        affected = update_player(_conn, int(sel_row["ID"]),
                                                 u_name.strip(), u_country.strip(), u_role,
                                                 int(u_matches), int(u_runs), int(u_wickets))
                        if affected == 0:
                            st.warning("⚠️ Player not found.")
                        else:
                            st.success(f"✅ **{u_name}** updated successfully!")
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Failed to update: {e}")

    # ════════════════════════ DELETE ═════════════════════════════════════════
    with tab_delete:
        st.markdown("##### Select a player to delete")

        if not player_labels:
            st.info("No players in the database yet.")
        else:
            del_label = st.selectbox("Select Player to Delete", player_labels, key="del_select")
            del_row   = player_options[del_label]

            # Preview card
            st.markdown(f"""
            | Field   | Value |
            |---------|-------|
            | **ID**      | {del_row['ID']} |
            | **Name**    | {del_row['Name']} |
            | **Country** | {del_row['Country']} |
            | **Role**    | {del_row['Role']} |
            | **Matches** | {del_row['Matches']} |
            | **Runs**    | {del_row['Runs']} |
            | **Wickets** | {del_row['Wickets']} |
            """)

            st.warning(f"⚠️ You are about to delete **{del_row['Name']}**. This cannot be undone.")
            if st.button("�️ Confirm Delete", type="primary", use_container_width=True):
                try:
                    affected = delete_player(_conn, int(del_row["ID"]))
                    if affected == 0:
                        st.warning("⚠️ Player not found.")
                    else:
                        st.success(f"✅ **{del_row['Name']}** deleted successfully!")
                        st.rerun()
                except Exception as e:
                    st.error(f"❌ Failed to delete: {e}")
