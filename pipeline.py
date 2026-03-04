import pandas as pd
import requests
import os
import json
import time
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text

load_dotenv()

print("PIPELINE FILE RUNNING 🚀")
API_KEY = os.getenv("API_KEY")

if not API_KEY:
    API_KEY = "DUMMY_KEY"
    print("Warning: API key not found. Relying on local data.")

headers = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

from sqlalchemy import create_engine
from urllib.parse import quote_plus

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = None

try:
    encoded_password = quote_plus(DB_PASSWORD)

    DATABASE_URL = (
        f"postgresql+psycopg2://{DB_USER}:{encoded_password}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    engine = create_engine(DATABASE_URL)

 

except Exception as e:
    print(f" Engine creation failed: {e}")


def safe_api_call(url, headers, params=None):

    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)

        if response.status_code != 200:
            print(f" API failed: {response.status_code}")
            return {}

        return response.json()

    except Exception as e:
        print(" API error:", e)
        return {}

## RECENT MATCHES:

def recent_match_data():

    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"

    data = safe_api_call(url, headers)

    if not data:
        print(" No recent match data")
        return pd.DataFrame()
    recent_values = data.get("typeMatches", [])

    recent_match_info = []

    if recent_values:
        series_matches = recent_values[0].get("seriesMatches", [])

        if series_matches:
            matches = series_matches[0]["seriesAdWrapper"].get("matches", [])

            for item in matches:
                match_info = item.get("matchInfo")
                if match_info:
                    recent_match_info.append(match_info)

    matches_list = []

    for info in recent_match_info:

        match_dict = {
            "Team1_ID": info['team1']['teamId'],
            "Team2_ID": info['team2']['teamId'],
            "Team 1": info['team1']['teamName'],
            "Team 2": info['team2']['teamName'],
            "Match ID": info['matchId'],
            "Series ID": info['seriesId'],
            "Series Name": info['seriesName'],
            "Match Description": info['matchDesc'],
            "Match Format": info['matchFormat'],
            "Start Date": info['startDate'],
            "End Date": info['endDate'],
            "State": info['state'],
            "Status": info['status'],
            "Venue": info['venueInfo']['ground'] + ", " + info['venueInfo']['city']
        }

        matches_list.append(match_dict)

    matches_df = pd.DataFrame(matches_list)

    if not matches_df.empty:

        matches_df["Start Date"] = pd.to_datetime(
            matches_df["Start Date"].astype("int64"),
            unit="ms"
        )

        matches_df["End Date"] = pd.to_datetime(
            matches_df["End Date"].astype("int64"),
            unit="ms"
        )

        matches_df = matches_df.sort_values("Start Date", ascending=False)
        matches_df.reset_index(drop=True, inplace=True)
        matches_df.index = matches_df.index + 1
        matches_df = matches_df.fillna("-")

    return matches_df


#LIVE MATCHES:


def live_data():

    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/live"

    data = safe_api_call(url, headers)

    if not data:
        print(" No live match data")
        return pd.DataFrame()
    type_matches = data.get("typeMatches", [])

    matches_list = []

    for match_type in type_matches:

        series_matches = match_type.get("seriesMatches", [])

        for item in series_matches:

            series_wrapper = item.get('seriesAdWrapper')

            if not series_wrapper:
                continue

            matches = series_wrapper.get('matches', [])

            for match in matches:

                info = match.get('matchInfo', {})

                match_dict = {
                    "Match ID": info.get('matchId'),
                    "Series Name": info.get('seriesName'),
                    "Match": info.get('matchDesc'),
                    "Format": info.get('matchFormat'),
                    "Team 1": info.get('team1', {}).get('teamName'),
                    "Team 2": info.get('team2', {}).get('teamName'),
                    "Venue": info.get('venueInfo', {}).get('ground'),
                    "City": info.get('venueInfo', {}).get('city'),
                    "State": info.get('state'),
                    "Status": info.get('status'),
                    "Start Date": info.get('startDate'),
                    "End Date": info.get('endDate')
                }

                score = match.get('matchScore')

                if score:

                    team1 = score.get('team1Score', {}).get('inngs1', {})
                    team2 = score.get('team2Score', {}).get('inngs1', {})

                    match_dict["Team1 Runs"] = int(team1.get('runs', 0))
                    match_dict["Team1 Wickets"] = int(team1.get('wickets', 0))
                    match_dict["Team1 Overs"] = float(team1.get('overs', 0))

                    match_dict["Team2 Runs"] = int(team2.get('runs', 0))
                    match_dict["Team2 Wickets"] = int(team2.get('wickets', 0))
                    match_dict["Team2 Overs"] = float(team2.get('overs', 0))

                else:

                    match_dict["Team1 Runs"] = None
                    match_dict["Team1 Wickets"] = None
                    match_dict["Team1 Overs"] = None
                    match_dict["Team2 Runs"] = None
                    match_dict["Team2 Wickets"] = None
                    match_dict["Team2 Overs"] = None

                matches_list.append(match_dict)

    matches_df = pd.DataFrame(matches_list)

    if not matches_df.empty:

        matches_df["Start Date"] = pd.to_datetime(
            matches_df["Start Date"].astype("int64"),
            unit="ms"
        )

        matches_df["End Date"] = pd.to_datetime(
            matches_df["End Date"].astype("int64"),
            unit="ms"
        )

        matches_df = matches_df.sort_values("Start Date", ascending=False)
        matches_df.reset_index(drop=True, inplace=True)
        matches_df.index = matches_df.index + 1
        matches_df = matches_df.fillna("-")

    return matches_df


#UPCOMING MATCHES:

def upcoming_data():

    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/upcoming"

    data = safe_api_call(url, headers)

    if not data:
        print(" No upcoming data")
        return pd.DataFrame()

    type_matches = data.get("typeMatches", [])

    upcoming_match_info = []

    if type_matches:

        series_matches = type_matches[0].get("seriesMatches", [])

        if series_matches:

            matches = series_matches[0]["seriesAdWrapper"].get("matches", [])

            for item in matches:

                match_info = item.get("matchInfo")

                if match_info:
                    upcoming_match_info.append(match_info)

    clean_upcoming = []

    for match in upcoming_match_info:

        match_dict = {
            "Match ID": match.get("matchId"),
            "Series Name": match.get("seriesName"),
            "Match": match.get("matchDesc"),
            "Format": match.get("matchFormat"),
            "Team 1": match.get("team1", {}).get("teamName"),
            "Team 2": match.get("team2", {}).get("teamName"),
            "Venue": match.get("venueInfo", {}).get("ground"),
            "City": match.get("venueInfo", {}).get("city"),
            "State": match.get("state"),
            "Status": match.get("status"),
            "Start Date": match.get("startDate")
        }

        clean_upcoming.append(match_dict)

    upcoming_df = pd.DataFrame(clean_upcoming)

    if not upcoming_df.empty:

        upcoming_df["Start Date"] = pd.to_datetime(
            upcoming_df["Start Date"].astype("int64"),
            unit="ms"
        )

        upcoming_df = upcoming_df.sort_values("Start Date")
        upcoming_df.reset_index(drop=True, inplace=True)
        upcoming_df.index = upcoming_df.index + 1
        upcoming_df = upcoming_df.fillna("-")

    return upcoming_df



## Question 1 Find all players who represent India. Display their full name, 
# playing role, batting style, and bowling style. 

def get_q1_india_players():

    team_id = 2
    team_name = "India"


    url = f"https://cricbuzz-cricket.p.rapidapi.com/teams/v1/{team_id}/players"
    data = safe_api_call(url, headers)

    if not data:
        return pd.DataFrame()

    player_list = data.get("player", [])
    final_players = []
    current_role = "Unknown"

    for p in player_list:
        if "id" not in p:
            current_role = p.get("name")
        else:
            final_players.append({
                "player_id": p.get("id"),
                "name": p.get("name"),
                "role": current_role,
                "batting_style": p.get("battingStyle"),
                "bowling_style": p.get("bowlingStyle"),
                "country": team_name
            })

    df = pd.DataFrame(final_players).fillna("N/A")

    with engine.begin() as conn:
        df.to_sql(
            "q1_players",
            conn,
            if_exists="replace",
            index=False
        )

    print("Q1 table loaded ")

    return df


## Question 2 Show all cricket matches that were played in the last Few days. Include the match description, both team names, 
## venue name with city, and the match date. Sort by most recent matches first.


def get_q2_recent_matches():

    # ---------------- STEP 1: FETCH FROM API ----------------
    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"
    data = safe_api_call(url, headers)

    records = []

    for type_grp in data.get("typeMatches", []):
        for series_grp in type_grp.get("seriesMatches", []):
            wrapper = series_grp.get("seriesAdWrapper", {})
            for match in wrapper.get("matches", []):

                mi = match.get("matchInfo", {})

                records.append({
                    "match_desc": mi.get("matchDesc"),
                    "team1": mi.get("team1", {}).get("teamName"),
                    "team2": mi.get("team2", {}).get("teamName"),
                    "venue": mi.get("venueInfo", {}).get("ground"),
                    "city": mi.get("venueInfo", {}).get("city"),
                    "start_date_raw": mi.get("startDate")
                })

    df_raw = pd.DataFrame(records)

    if df_raw.empty:
        return pd.DataFrame()

    # ---------------- STEP 2: CLEAN DATE ----------------
    df_raw["start_date"] = pd.to_datetime(
        pd.to_numeric(df_raw["start_date_raw"], errors="coerce"),
        unit="ms"
    )

    df_raw = df_raw.drop(columns=["start_date_raw"])
    df_raw = df_raw.fillna("N/A")

    # ---------------- STEP 3: STORE  ----------------
    with engine.begin() as conn:
        df_raw.to_sql(
            "q2_recent_matches",
            conn,
            if_exists="replace",
            index=False
        )

    # ---------------- STEP 4: SQL QUERY ----------------
    query = """
    SELECT
        match_desc,
        team1 || ' vs ' || team2 AS teams,
        venue || ', ' || city AS venue,
        start_date
    FROM q2_recent_matches
    ORDER BY start_date DESC
    """

    with engine.connect() as conn:
        df_result = pd.read_sql(query, conn)

    return df_result

## Question 3 List the top 10 highest run scorers in ODI cricket. Show player name, 
## total runs scored, batting average, and number of centuries. Display the highest run scorer first.
def get_q3_top_odi_scorers():

    # ---------------- STEP 1: GET TEAM PLAYERS (India) ----------------
    team_url = "https://cricbuzz-cricket.p.rapidapi.com/teams/v1/2/players"
    team_data = safe_api_call(team_url, headers)

    player_ids = []

    for p in team_data.get("player", []):
        if "id" in p:
            player_ids.append({
                "id": p["id"],
                "name": p["name"]
            })

    records = []

    # ---------------- STEP 2: FETCH ODI STATS ----------------
    for player in player_ids:

        url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player['id']}/batting"
        data = safe_api_call(url, headers)

        headers_list = data.get("headers", [])

        if "ODI" not in headers_list:
            continue

        idx = headers_list.index("ODI")

        runs = 0
        avg = 0
        cents = 0

        for row in data.get("values", []):
            values = row.get("values", [])
            key = values[0] if values else ""
            val = values[idx] if len(values) > idx else "0"

            try:
                num = float(str(val).replace("*", ""))
            except:
                num = 0

            if key == "Runs":
                runs = int(num)
            elif key == "Average":
                avg = float(num)
            elif key == "100s":
                cents = int(num)

        if runs > 0:
            records.append({
                "player": player["name"],
                "runs": runs,
                "average": avg,
                "centuries": cents
            })

    df_raw = pd.DataFrame(records)

    if df_raw.empty:
        return pd.DataFrame()

    # ---------------- STEP 3: STORE ----------------
    with engine.begin() as conn:
        df_raw.to_sql(
            "q3_odi_batting",
            conn,
            if_exists="replace",
            index=False
        )

    # ---------------- STEP 4: SQL QUERY ----------------
    query = """
    SELECT
        player,
        runs,
        average,
        centuries
    FROM q3_odi_batting
    ORDER BY runs DESC
    LIMIT 10
    """

    with engine.connect() as conn:
        df_result = pd.read_sql(query, conn)

    return df_result

## Question 4 Display all cricket venues that have a seating capacity of more than 
## 25,000 spectators. Show venue name, city, country, and capacity. 
## Order by largest capacity first (10 Venues enough).

def get_q4_large_venues():

    # ---------------- STEP 1: FETCH RECENT MATCHES ----------------
    matches_url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"
    data = safe_api_call(matches_url, headers)

    venue_ids = set()

    for type_grp in data.get("typeMatches", []):
        for series_grp in type_grp.get("seriesMatches", []):
            wrapper = series_grp.get("seriesAdWrapper", {})
            for match in wrapper.get("matches", []):
                mi = match.get("matchInfo", {})
                v_id = mi.get("venueInfo", {}).get("id")
                if v_id:
                    venue_ids.add(v_id)

    # ---------------- STEP 2: FETCH VENUE DETAILS ----------------
    records = []
    base_url = "https://cricbuzz-cricket.p.rapidapi.com/venues/v1/{}"

    for v_id in venue_ids:

        try:
            v_url = base_url.format(v_id)
            v_info = safe_api_call(v_url, headers)

            capacity_raw = str(v_info.get("capacity", "0")).replace(",", "")
            capacity = int(capacity_raw) if capacity_raw.isdigit() else 0

            records.append({
                "venue_name": v_info.get("ground"),
                "city": v_info.get("city"),
                "country": v_info.get("country"),
                "capacity": capacity
            })

        except Exception:
            continue

    df_raw = pd.DataFrame(records).fillna("N/A")

    if df_raw.empty:
        return pd.DataFrame()

    # ---------------- STEP 3: STORE ----------------
    with engine.begin() as conn:
        df_raw.to_sql(
            "q4_venues",
            conn,
            if_exists="replace",
            index=False
        )

    # ---------------- STEP 4: SQL QUERY ----------------
    query = """
    SELECT
        venue_name,
        city,
        country,
        capacity
    FROM q4_venues
    WHERE capacity > 25000
    ORDER BY capacity DESC
    LIMIT 10
    """

    with engine.connect() as conn:
        df_result = pd.read_sql(query, conn)

    return df_result


## Question 5 Calculate how many matches each team has won. 
# Show team name and total number of wins. Display teams with the most wins first.

from sqlalchemy import text
def get_q5_team_win_counts():


    # =========================
    # STEP 1 — API FETCH
    # =========================
    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"
    data = safe_api_call(url, headers)

    if not data:
        print(" API returned empty")
        return pd.DataFrame()

    matches = []

    for mtype in data.get("typeMatches", []):
        for series in mtype.get("seriesMatches", []):
            wrapper = series.get("seriesAdWrapper", {})
            for match in wrapper.get("matches", []):
                info = match.get("matchInfo", {})

                status_val = info.get("status")

                # Ensure status is string ONLY
                if not isinstance(status_val, str):
                    status_val = str(status_val)

                matches.append({
                    "status": status_val
                })

    if not matches:
        print("No matches extracted")
        return pd.DataFrame()

    df_raw = pd.DataFrame(matches)

    print("Raw DF Head:")
    print(df_raw.head())
    print("Dtypes:")
    print(df_raw.dtypes)

    # =========================
    # STEP 2 — STORE 
    # =========================
    with engine.begin() as conn:
        df_raw.to_sql(
            "q5_matches",
            conn,
            if_exists="replace",
            index=False
        )

    

    # =========================
    # STEP 3 —  SQL
    # =========================
    query = """
        SELECT
            INITCAP(SPLIT_PART(status, ' won ', 1)) AS team,
            COUNT(*) AS wins
        FROM q5_matches
        WHERE status ILIKE '% won %'
        GROUP BY INITCAP(SPLIT_PART(status, ' won ', 1))
        ORDER BY wins DESC
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
        print("SQL rows:", rows)

   
    df_result = pd.DataFrame(rows, columns=["team", "wins"])



    return df_result

## Question 6 Count how many players belong to each playing role (like Batsman, Bowler, All-rounder, Wicket-keeper). 
## Show the role and count of players for each role.
# Question 6 Count how many players belong to each playing role

def get_q6_players_by_role():

    

    # =====================================
    # STEP 1 — CHECK IF TABLE EXISTS
    # =====================================

    check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'q6_players_role'
        );
    """

    with engine.connect() as conn:
        table_exists = conn.execute(text(check_query)).scalar()

    # =====================================
    # STEP 2 — FETCH FROM API IF NOT EXISTS
    # =====================================

    if not table_exists:

        

        url = "https://cricbuzz-cricket.p.rapidapi.com/teams/v1/international"
        data = safe_api_call(url, headers)

        if not data:
            return pd.DataFrame()

        teams = data.get("list", [])
        team_ids = [int(t["teamId"]) for t in teams if t.get("teamId")]

        all_players = []

        for tid in team_ids:

            team_url = f"https://cricbuzz-cricket.p.rapidapi.com/teams/v1/{tid}/players"
            team_data = safe_api_call(team_url, headers)

            if not team_data:
                continue

            current_role = None

            for p in team_data.get("player", []):

                if "id" not in p:
                    current_role = p.get("name")
                else:
                    all_players.append({
                        "player_id": int(p.get("id")),
                        "name": p.get("name"),
                        "team_id": tid,
                        "role": str(current_role).title()
                    })

        if not all_players:
            return pd.DataFrame()

        df_store = pd.DataFrame(all_players)

        # Store cleanly using connection
        with engine.begin() as conn:
            df_store.to_sql(
                "q6_players_role",
                conn,
                if_exists="replace",
                index=False
            )

        

    # =====================================
    # STEP 3 — SQL AGGREGATION
    # =====================================

    query = """
        SELECT
            role AS Role,
            COUNT(*) AS Player_Count
        FROM q6_players_role
        GROUP BY role
        ORDER BY COUNT(*) DESC
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()

    df_result = pd.DataFrame(rows, columns=["Role", "Player_Count"])

   

    return df_result

    
## Question 7 Find the highest individual batting score achieved in each cricket format
## (Test, ODI, T20I). Display the format and the highest score for that format


from sqlalchemy import text

def get_q7_highest_scores():


    # =====================================
    # STEP 1 — CHECK TABLE EXISTS
    # =====================================
    check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'q7_highest_scores'
        );
    """

    with engine.connect() as conn:
        table_exists = conn.execute(text(check_query)).scalar()

    # =====================================
    # STEP 2 — BUILD IF NOT EXISTS
    # =====================================
    if not table_exists:


        url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"
        data = safe_api_call(url, headers)

        if not data:
            return pd.DataFrame()

        match_rows = []

        for type_match in data.get("typeMatches", []):
            for series in type_match.get("seriesMatches", []):
                wrapper = series.get("seriesAdWrapper")
                if not wrapper:
                    continue

                for match in wrapper.get("matches", []):
                    info = match.get("matchInfo", {})

                    if info.get("state") != "Complete":
                        continue

                    fmt = info.get("matchFormat")

                    if fmt == "T20":
                        fmt = "T20I"

                    match_rows.append({
                        "match_id": info.get("matchId"),
                        "format": fmt
                    })

        df_matches = pd.DataFrame(match_rows).drop_duplicates()

        if df_matches.empty:
            return pd.DataFrame()

        

        batting_rows = []

        for _, row in df_matches.iterrows():

            match_id = row["match_id"]
            fmt = row["format"]

            score_url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}/scard"
            score_data = safe_api_call(score_url, headers)

            if not score_data:
                continue

            for innings in score_data.get("scorecard", []):
                for b in innings.get("batsman", []):

                    runs_val = b.get("runs", 0)
                    try:
                        runs_val = int(runs_val)
                    except:
                        runs_val = 0

                    batting_rows.append({
                        "match_id": match_id,
                        "format": fmt,
                        "player_name": b.get("name"),
                        "runs": runs_val
                    })

            time.sleep(0.2)

        df_batting = pd.DataFrame(batting_rows)

        if df_batting.empty:
            return pd.DataFrame()

        # Store raw batting
        with engine.begin() as conn:
            df_batting.to_sql(
                "q7_match_batting",
                conn,
                if_exists="replace",
                index=False
            )

        # Aggregate highest scores
        agg_query = """
            SELECT
                format,
                MAX(runs) AS highest_score
            FROM q7_match_batting
            GROUP BY format
        """

        with engine.connect() as conn:
            result = conn.execute(text(agg_query))
            rows = result.fetchall()

        df_result = pd.DataFrame(rows, columns=["format", "highest_score"])

        # Store final analytics table
        with engine.begin() as conn:
            df_result.to_sql(
                "q7_highest_scores",
                conn,
                if_exists="replace",
                index=False
            )


    # =====================================
    # STEP 3 — FINAL OUTPUT
    # =====================================

    final_query = """
        SELECT
            format AS "Format",
            highest_score AS "Highest Individual Score"
        FROM q7_highest_scores
        ORDER BY format
    """

    with engine.connect() as conn:
        result = conn.execute(text(final_query))
        rows = result.fetchall()

    df_final = pd.DataFrame(rows, columns=["Format", "Highest Individual Score"])

   

    return df_final

## Question 8 Show all cricket series that started in the year 2024. Include series name,
## host country, match type, start date, and total number of matches planned.


def get_q8_series_2024():


    # =====================================
    # STEP 1 — CHECK CACHE
    # =====================================

    check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'q8_series_2024'
        );
    """

    with engine.connect() as conn:
        table_exists = conn.execute(text(check_query)).scalar()

    if table_exists:
        
        final_query = """
            SELECT
                series_name AS "Series Name",
                host_country AS "Host Country",
                match_type AS "Match Type",
                start_date AS "Start Date",
                total_matches AS "Total Matches"
            FROM q8_series_2024
            ORDER BY start_date
        """
        with engine.connect() as conn:
            result = conn.execute(text(final_query))
            rows = result.fetchall()

        return pd.DataFrame(
            rows,
            columns=[
                "Series Name",
                "Host Country",
                "Match Type",
                "Start Date",
                "Total Matches"
            ]
        )

    # =====================================
    # STEP 2 — FETCH ARCHIVES 
    # =====================================

    archive_urls = [
        "https://cricbuzz-cricket.p.rapidapi.com/series/v1/archives/international",
        "https://cricbuzz-cricket.p.rapidapi.com/series/v1/archives/league",
        "https://cricbuzz-cricket.p.rapidapi.com/series/v1/archives/domestic",
        "https://cricbuzz-cricket.p.rapidapi.com/series/v1/archives/women"
    ]

    all_series = []

    for url in archive_urls:
        data = safe_api_call(url, headers, params={"year": "2024"})
        if not data:
            continue

        for block in data.get("seriesMapProto", []):
            for s in block.get("series", []):

                start_dt = s.get("startDt")
                if not start_dt:
                    continue

                start_date = pd.to_datetime(
                    int(start_dt),
                    unit="ms",
                    errors="coerce"
                )

                if start_date.year != 2024:
                    continue

                all_series.append({
                    "series_id": s.get("id"),
                    "series_name": s.get("name"),
                    "start_date": start_date
                })

    df_series = pd.DataFrame(all_series)

    if df_series.empty:
        return pd.DataFrame()

   
    # df_series = df_series.head(25)
    print("Total 2024 series found before enrichment:", len(df_series))
    print(df_series["start_date"].min(), "to", df_series["start_date"].max())
    #print(f"Enriching {len(df_series)} series...")

    # =====================================
    # STEP 3 — ENRICH 
    # =====================================

    enriched_rows = []

    for _, row in df_series.iterrows():

        sid = int(row["series_id"])

        detail_url = f"https://cricbuzz-cricket.p.rapidapi.com/series/v1/{sid}"
        detail_data = safe_api_call(detail_url, headers)

        if not detail_data:
            continue

        total_matches = 0
        formats = set()
        host_country = None

        for block in detail_data.get("matchDetails", []):
            for m in block.get("matchDetailsMap", {}).get("match", []):

                total_matches += 1

                fmt = m.get("matchInfo", {}).get("matchFormat")
                if fmt:
                    formats.add(fmt)

                venue = m.get("matchInfo", {}).get("venueInfo", {})
                if venue and not host_country:
                    host_country = venue.get("country")

        match_type = "/".join(sorted(formats)) if formats else None

        enriched_rows.append({
            "series_name": row["series_name"],
            "host_country": host_country,
            "match_type": match_type,
            "start_date": row["start_date"],
            "total_matches": total_matches
        })

        time.sleep(0.05)

    df_final = pd.DataFrame(enriched_rows)

    if df_final.empty:
        return pd.DataFrame()

    # =====================================
    # STEP 4 — STORE CACHE
    # =====================================

    with engine.begin() as conn:
        df_final.to_sql(
            "q8_series_2024",
            conn,
            if_exists="replace",
            index=False
        )



    return df_final.sort_values("start_date").reset_index(drop=True)

## Question 9 Find all-rounder players who have scored more than 
## 1000 runs AND taken more than 50 wickets in their career.
# Display player name, total runs, total wickets, and the cricket format.


from sqlalchemy import text

def get_q9_allrounders():

    import pandas as pd
    import requests
    import time

    try:

        # =============================
        # STEP 1 — CHECK CACHE
        # =============================

        check_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'q9_result'
            );
        """

        with engine.connect() as conn:
            table_exists = conn.execute(text(check_query)).scalar()

        if table_exists:
            return pd.read_sql("SELECT * FROM q9_result", engine)


        # =============================
        # STEP 2 — FETCH TEAMS
        # =============================

        teams_url = "https://cricbuzz-cricket.p.rapidapi.com/teams/v1/international"
        resp = requests.get(teams_url, headers=headers, timeout=20)

        if resp.status_code != 200:
            return pd.DataFrame()

        teams = []
        for t in resp.json().get("list", []):
            if t.get("teamId"):
                teams.append((t["teamId"], t["teamName"]))


        batting_records = []
        bowling_records = []
        processed_players = set()


        # =============================
        # STEP 3 — FETCH ALL ALLROUNDERS
        # =============================

        for team_id, team_name in teams:

            team_url = f"https://cricbuzz-cricket.p.rapidapi.com/teams/v1/{team_id}/players"
            team_resp = requests.get(team_url, headers=headers, timeout=20)

            if team_resp.status_code != 200:
                continue

            players_section = team_resp.json().get("player", [])
            current_role = None

            for item in players_section:

                if "id" not in item:
                    header = item.get("name", "").upper()
                    current_role = "ALLROUNDER" if "ALL" in header else None
                    continue

                if current_role != "ALLROUNDER":
                    continue

                player_id = item.get("id")
                player_name = item.get("name")

                if not player_id or player_id in processed_players:
                    continue

                processed_players.add(player_id)

                # ---------- Batting ----------
                bat_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/batting"
                bat_resp = requests.get(bat_url, headers=headers, timeout=20)

                if bat_resp.status_code == 200:

                    bat_data = bat_resp.json()
                    formats = bat_data.get("headers", [])[1:]

                    runs_row = None
                    for r in bat_data.get("values", []):
                        if r["values"][0] == "Runs":
                            runs_row = r["values"][1:]
                            break

                    if runs_row:
                        for fmt, runs in zip(formats, runs_row):
                            try:
                                batting_records.append({
                                    "player_id": player_id,
                                    "player_name": player_name,
                                    "team_name": team_name,
                                    "format": fmt,
                                    "total_runs": int(runs)
                                })
                            except:
                                continue


                # ---------- Bowling ----------
                bowl_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/bowling"
                bowl_resp = requests.get(bowl_url, headers=headers, timeout=20)

                if bowl_resp.status_code == 200:

                    bowl_data = bowl_resp.json()
                    formats = bowl_data.get("headers", [])[1:]

                    wickets_row = None
                    for r in bowl_data.get("values", []):
                        if r["values"][0] == "Wickets":
                            wickets_row = r["values"][1:]
                            break

                    if wickets_row:
                        for fmt, wkts in zip(formats, wickets_row):
                            try:
                                bowling_records.append({
                                    "player_id": player_id,
                                    "player_name": player_name,
                                    "team_name": team_name,
                                    "format": fmt,
                                    "total_wickets": int(wkts)
                                })
                            except:
                                continue

                time.sleep(0.15)


        df_bat = pd.DataFrame(batting_records)
        df_bowl = pd.DataFrame(bowling_records)

        if df_bat.empty or df_bowl.empty:
            return pd.DataFrame()


        # =============================
        # STEP 4 — STORE RAW TABLES
        # =============================

        with engine.begin() as conn:
            df_bat.to_sql("q9_batting_stats", conn, if_exists="replace", index=False)
            df_bowl.to_sql("q9_bowling_stats", conn, if_exists="replace", index=False)


        # =============================
        # STEP 5 —  SQL ANALYTICS
        # =============================

        query = text("""
            SELECT
                b.player_name,
                b.team_name,
                b.format,
                b.total_runs,
                bw.total_wickets
            FROM q9_batting_stats b
            JOIN q9_bowling_stats bw
                ON b.player_id = bw.player_id
                AND b.format = bw.format
            WHERE b.total_runs > 1000
            AND bw.total_wickets > 50
            ORDER BY b.player_name
        """)

        with engine.connect() as conn:
            result = conn.execute(query)
            rows = result.fetchall()

        df_result = pd.DataFrame(
            rows,
            columns=[
                "player_name",
                "team_name",
                "format",
                "total_runs",
                "total_wickets"
            ]
        )


        # =============================
        # STEP 6 — STORE FINAL RESULT
        # =============================

        with engine.begin() as conn:
            df_result.to_sql("q9_result", conn, if_exists="replace", index=False)

        return df_result


    except Exception as e:
        print("Error in Q9:", e)
        return pd.DataFrame()
    
#---------------------------------------------------------------------------------------------------------------------------


## Question 10 Get details of the last 20 completed matches. 
# Show match description, both team names,
## winning team, victory margin, victory type (runs/wickets),
#  and venue name. Display the most recent matches first.
from sqlalchemy import text
import pandas as pd
import re

def get_q10_last_20_completed_matches():

    # =====================================================
    # STEP 1 — FETCH
    # =====================================================
    try:
        query = """
            SELECT *
            FROM que_10_recent_matches
            ORDER BY start_date DESC
            LIMIT 20
        """

        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()

        if rows:
            df_cached = pd.DataFrame(
                rows,
                columns=[
                    "match_description",
                    "team_1",
                    "team_2",
                    "winning_team",
                    "victory_margin",
                    "victory_type",
                    "venue",
                    "start_date"
                ]
            )

            df_cached["start_date"] = pd.to_datetime(df_cached["start_date"])
            df_cached["match_date"] = df_cached["start_date"].dt.strftime("%d %b %Y")

            return df_cached.drop(columns=["start_date"])

    except Exception:
        
        pass


    # =====================================================
    # STEP 2 — FETCH FROM API
    # =====================================================
    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"
    data = safe_api_call(url, headers)

    if not data:
        return pd.DataFrame()

    records = []

    for type_grp in data.get("typeMatches", []):
        for series in type_grp.get("seriesMatches", []):
            wrapper = series.get("seriesAdWrapper", {})

            for match in wrapper.get("matches", []):
                mi = match.get("matchInfo", {})
                status = mi.get("status", "")

                if not isinstance(status, str):
                    continue

                if "won" not in status.lower():
                    continue

                team1 = mi.get("team1", {}).get("teamName")
                team2 = mi.get("team2", {}).get("teamName")

                venue_info = mi.get("venueInfo", {})
                ground = venue_info.get("ground", "")
                city = venue_info.get("city", "")
                venue = f"{ground}, {city}".strip(", ")

                winner = status.split(" won ")[0].strip()

                run_win = re.search(r'won by (\d+) runs', status.lower())
                wkt_win = re.search(r'won by (\d+) (wickets|wkts)', status.lower())

                margin = None
                victory_type = None

                if run_win:
                    margin = int(run_win.group(1))
                    victory_type = "Runs"
                elif wkt_win:
                    margin = int(wkt_win.group(1))
                    victory_type = "Wickets"

                records.append({
                    "match_description": mi.get("matchDesc"),
                    "team_1": team1,
                    "team_2": team2,
                    "winning_team": winner,
                    "victory_margin": margin,
                    "victory_type": victory_type,
                    "venue": venue,
                    "start_date": pd.to_datetime(
                        pd.to_numeric(mi.get("startDate"), errors="coerce"),
                        unit="ms"
                    )
                })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # =====================================================
    # STEP 3 — SORT + REMOVE NULL MATCHES
    # =====================================================
    df = df.sort_values("start_date", ascending=False)

    df = df.dropna(subset=[
        "match_description",
        "team_1",
        "team_2",
        "winning_team",
        "victory_margin",
        "victory_type",
        "venue"
    ])

    df_top20 = df.head(20).reset_index(drop=True)

    if df_top20.empty:
        return pd.DataFrame()

    # =====================================================
    # STEP 4 — STORE CLEAN DATA INTO DATABASE
    # =====================================================
    try:
        with engine.begin() as conn:
            df_top20.to_sql(
                "que_10_recent_matches",
                conn,
                if_exists="replace",
                index=False
            )
    except Exception as e:
        print("DB Write Error:", e)

    # =====================================================
    # STEP 5 — FORMAT OUTPUT
    # =====================================================
    df_top20["match_date"] = df_top20["start_date"].dt.strftime("%d %b %Y")

    return df_top20.drop(columns=["start_date"])


#---------------------------------------------------------------------------------------------------------------------------------

## Question 11 Compare each player's performance across different cricket formats.
#  For players who have played at least 2 different formats, 
## show their total runs in Test cricket, ODI cricket, and T20I cricket, along with their overall batting average across all formats.

from sqlalchemy import text
import pandas as pd
import time


# ===============================
# INGESTION
# ===============================
def _build_que_11_raw_table():

    MAX_TEAMS = 6
    PLAYERS_PER_TEAM = 4

    teams_url = "https://cricbuzz-cricket.p.rapidapi.com/teams/v1/international"
    teams_data = safe_api_call(teams_url, headers)

    if not teams_data:
        return

    teams = [
        (t["teamId"], t["teamName"])
        for t in teams_data.get("list", [])
        if t.get("teamId")
    ][:MAX_TEAMS]

    records = []

    for team_id, team_name in teams:

        players_url = f"https://cricbuzz-cricket.p.rapidapi.com/teams/v1/{team_id}/players"
        players_data = safe_api_call(players_url, headers)

        if not players_data:
            continue

        count = 0

        for player in players_data.get("player", []):
            if count >= PLAYERS_PER_TEAM:
                break

            if "id" not in player:
                continue

            count += 1

            player_id = player["id"]
            player_name = player["name"]

            bat_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/batting"
            pdata = safe_api_call(bat_url, headers)

            if not pdata:
                continue

            headers_list = pdata.get("headers", [])
            if not headers_list:
                continue

            formats = headers_list[1:]

            runs_dict = {}
            avg_dict = {}

            for row_data in pdata.get("values", []):
                values = row_data.get("values", [])
                if not values:
                    continue

                label = values[0]
                stats_vals = values[1:]

                if label == "Runs":
                    runs_dict = dict(zip(formats, stats_vals))

                if label == "Average":
                    avg_dict = dict(zip(formats, stats_vals))

            for fmt in ["Test", "ODI", "T20"]:

                try:
                    runs = int(runs_dict.get(fmt, 0))
                except:
                    runs = 0

                try:
                    avg = float(avg_dict.get(fmt))
                except:
                    avg = None

                records.append({
                    "player_id": player_id,
                    "player_name": player_name,
                    "team_name": team_name,
                    "format": fmt,
                    "runs": runs,
                    "average": avg
                })

            time.sleep(0.05)

    df_raw = pd.DataFrame(records)

    if df_raw.empty:
        return

    with engine.begin() as conn:
        df_raw.to_sql(
            "que_11_player_batting_raw",
            conn,
            if_exists="replace",
            index=False
        )


# =====================================================
# MAIN PIPELINE FUNCTION (SQL ANALYTICS)
# =====================================================
def get_q11_player_format_comparison():

    # ----------------------------------------------
    # Step 1 — Ensure Raw Table Exists
    # ----------------------------------------------
    try:
        check_query = "SELECT 1 FROM que_11_player_batting_raw LIMIT 1"

        with engine.connect() as conn:
            conn.execute(text(check_query))

    except Exception:
        
        _build_que_11_raw_table()


    # ----------------------------------------------
    # Step 2 — SQL Analytics 
    # ----------------------------------------------
    query = """
        SELECT
            player_name,
            team_name,
            SUM(CASE WHEN format = 'Test' THEN runs ELSE 0 END) AS test_runs,
            SUM(CASE WHEN format = 'ODI' THEN runs ELSE 0 END) AS odi_runs,
            SUM(CASE WHEN format = 'T20' THEN runs ELSE 0 END) AS t20_runs,
            ROUND(AVG(average)::numeric, 2) AS overall_avg
        FROM que_11_player_batting_raw
        GROUP BY player_name, team_name
        HAVING COUNT(DISTINCT format) FILTER (WHERE runs > 0) >= 2
        ORDER BY overall_avg DESC;
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()

    df = pd.DataFrame(rows, columns=[
        "player_name",
        "team_name",
        "test_runs",
        "odi_runs",
        "t20_runs",
        "overall_avg"
    ])

    return df

## Question 12 Analyze each international team's performance when playing at home versus playing away. 
## Determine whether each team played at home or away based on whether the venue country matches the team's country. 
## Count wins for each team in both home and away conditions.


# =====================================================
# Question 12
# Analyze each international team's performance
# when playing at home versus away.
# Dataset: ICC Cricket World Cup 2023 (Series ID: 6732)
# =====================================================

from sqlalchemy import text
import pandas as pd


# ============================
#  INGESTION 
# ============================
def _build_que_12_information():

    url = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/6732"
    data = safe_api_call(url, headers)

    if not data:
        return

    records = []

    for item in data.get("matchDetails", []):

        match_map = item.get("matchDetailsMap", {})
        matches = match_map.get("match", [])

        for match in matches:

            mi = match.get("matchInfo", {})
            status = mi.get("status", "")

            # Only completed matches with winner
            if not isinstance(status, str):
                continue

            if "won" not in status.lower():
                continue

            team1 = mi.get("team1", {}).get("teamName")
            team2 = mi.get("team2", {}).get("teamName")

           
            venue_country = "INDIA"

            records.append({
                "team1_name": team1,
                "team2_name": team2,
                "status": status,
                "venue_country": venue_country
            })

    df_raw = pd.DataFrame(records)

    if df_raw.empty:
        return

    with engine.begin() as conn:
        df_raw.to_sql(
            "que_12_information",
            conn,
            if_exists="replace",
            index=False
        )


# =====================================================
# MAIN ANALYTICS FUNCTION (SQL DRIVEN)
# =====================================================
def get_que12_home_away_analysis():

    # ----------------------------------------------
    # Step 1 — Ensure raw table exists
    # ----------------------------------------------
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1 FROM que_12_information LIMIT 1"))
    except Exception:
        _build_que_12_information()

    # ----------------------------------------------
    # Step 2 — SQL Analytics
    # ----------------------------------------------
    query = """
        WITH expanded AS (

            -- Team 1 rows
            SELECT
                UPPER(TRIM(team1_name)) AS team,
                CASE
                    WHEN UPPER(TRIM(team1_name)) = UPPER(TRIM(venue_country))
                    THEN 'Home'
                    ELSE 'Away'
                END AS condition,
                UPPER(TRIM(SPLIT_PART(status, ' won', 1))) AS winner
            FROM que_12_information

            UNION ALL

            -- Team 2 rows
            SELECT
                UPPER(TRIM(team2_name)) AS team,
                CASE
                    WHEN UPPER(TRIM(team2_name)) = UPPER(TRIM(venue_country))
                    THEN 'Home'
                    ELSE 'Away'
                END AS condition,
                UPPER(TRIM(SPLIT_PART(status, ' won', 1))) AS winner
            FROM que_12_information
        )

        SELECT
            team AS "Team",
            SUM(
                CASE
                    WHEN condition = 'Home' AND team = winner THEN 1
                    ELSE 0
                END
            ) AS "Home Wins",
            SUM(
                CASE
                    WHEN condition = 'Away' AND team = winner THEN 1
                    ELSE 0
                END
            ) AS "Away Wins"
        FROM expanded
        GROUP BY team
        ORDER BY team
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()

    df = pd.DataFrame(rows, columns=["Team", "Home Wins", "Away Wins"])

    return df


## Question 13 Identify batting partnerships where two consecutive batsmen (batting positions next to each other) scored a 
## combined total of 100 or more runs in the same innings. Show both player names, 
## their combined partnership runs, and which innings it occurred in.


# ============================================================
# Question 13 — Century Partnerships
# ============================================================

def get_que13_century_partnerships():

    SERIES_ID = 3641

    # -------------------------
    # STEP 1 — GET MATCH IDS
    # -------------------------

    def get_match_ids(series_id):

        url = f"https://cricbuzz-cricket.p.rapidapi.com/series/v1/{series_id}"

        resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            print(" Series fetch failed")
            return []

        data = resp.json()

        match_ids = []

        for block in data.get("matchDetails", []):

            match_map = block.get("matchDetailsMap")

            if not match_map:
                continue

            for match in match_map.get("match", []):

                info = match.get("matchInfo", {})

                mid = info.get("matchId")

                if mid:
                    match_ids.append(mid)

        match_ids = list(set(match_ids))

        print(f" Found {len(match_ids)} matches")

        return match_ids


    # -------------------------
    # STEP 2 — LOAD API → DB
    # -------------------------

    match_ids = get_match_ids(SERIES_ID)

    with engine.begin() as conn:

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS q13_partnerships (
                match_id BIGINT,
                innings_id INT,
                player1 TEXT,
                player2 TEXT,
                runs INT
            );
        """))

        conn.execute(text("TRUNCATE TABLE q13_partnerships"))

        insert_sql = text("""
            INSERT INTO q13_partnerships
            (match_id, innings_id, player1, player2, runs)
            VALUES
            (:match_id, :innings_id, :player1, :player2, :runs)
        """)

        for mid in match_ids:

            url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{mid}/scard"

            resp = requests.get(url, headers=headers)

            if resp.status_code != 200:
                print(f" Failed match {mid}")
                continue

            data = resp.json()

            for innings in data.get("scorecard", []):

                iid = innings.get("inningsid")

                partnerships = (
                    innings.get("partnership", {})
                    .get("partnership", [])
                )

                for p in partnerships:

                    conn.execute(insert_sql,[ {
                        "match_id": mid,
                        "innings_id": iid,
                        "player1": p.get("bat1name"),
                        "player2": p.get("bat2name"),
                        "runs": p.get("totalruns", 0)
                    }])


    # -------------------------
    # STEP 3 — ANALYSIS
    # -------------------------

    query = """
        SELECT
            match_id AS "Match ID",
            player1 AS "Player 1",
            player2 AS "Player 2",
            runs AS "Combined Runs",
            innings_id AS "Innings"
        FROM q13_partnerships
        WHERE runs >= 100
        ORDER BY match_id, innings_id
    """

    df = pd.read_sql(query, engine)

    return df

## Question 14 Examine bowling performance at different venues. For bowlers who have played at least 3 matches at the same venue, 
## calculate their average economy rate, total wickets taken, and number of matches played at each venue. 
## Focus on bowlers who bowled at least 4 overs in each match.

# ============================================================
# Question 14 — Bowler Venue Performance
# ============================================================

def get_que14_bowler_venue_performance(series_id=3641):

    import requests
    import time
    import pandas as pd
    from sqlalchemy import text

    # ---------------------------
    # SERIES API — MATCH IDS + VENUES
    # ---------------------------

    series_url = f"https://cricbuzz-cricket.p.rapidapi.com/series/v1/{series_id}"

    resp = requests.get(series_url, headers=headers)

    if resp.status_code != 200:
        return pd.DataFrame(), f"Series {series_id}"

    data = resp.json()

    match_ids = []
    venue_map = {}
    series_name = f"Series {series_id}"

    for block in data.get("matchDetails", []):

        match_map = block.get("matchDetailsMap")

        if not match_map:
            continue

        for match in match_map.get("match", []):

            info = match.get("matchInfo", {})

            mid = info.get("matchId")

            venue_info = info.get("venueInfo", {})

            ground = venue_info.get("ground", "")
            city = venue_info.get("city", "")

            venue = f"{ground}, {city}".strip(", ")

            if mid:
                match_ids.append(mid)
                venue_map[mid] = venue if venue else "Unknown"

            if info.get("seriesName"):
                series_name = info.get("seriesName")

    match_ids = list(set(match_ids))

    # ---------------------------
    # CREATE TABLE
    # ---------------------------

    with engine.begin() as conn:

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS q14_bowling (
                match_id BIGINT,
                venue TEXT,
                bowler TEXT,
                overs FLOAT,
                runs INT,
                wickets INT
            );
        """))

        conn.execute(text("TRUNCATE TABLE q14_bowling"))

        insert_sql = text("""
            INSERT INTO q14_bowling
            (match_id, venue, bowler, overs, runs, wickets)
            VALUES
            (:match_id, :venue, :bowler, :overs, :runs, :wickets)
        """)

        # ---------------------------
        # SCORECARD API
        # ---------------------------

        for mid in match_ids:

            url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{mid}/scard"

            resp = requests.get(url, headers=headers, timeout=20)

            if resp.status_code != 200:
                continue

            data = resp.json()

            venue = venue_map.get(mid, "Unknown")

            for innings in data.get("scorecard", []):

                for bw in innings.get("bowler", []):

                    overs = float(bw.get("overs", 0))

                   
                    if overs < 4:
                        continue

                    conn.execute(insert_sql,[{
                        "match_id": mid,
                        "venue": venue,
                        "bowler": bw.get("name"),
                        "overs": overs,
                        "runs": int(bw.get("runs", 0)),
                        "wickets": int(bw.get("wickets", 0))
                    }])

            time.sleep(1)

    # ---------------------------
    # ANALYSIS
    # ---------------------------

    query = """
        SELECT
            bowler,
            venue,
            COUNT(*) AS matches,
            SUM(wickets) AS total_wickets,
            SUM(runs) AS runs,
            SUM(overs) AS overs
        FROM q14_bowling
        GROUP BY bowler, venue
        HAVING COUNT(*) >= 3
        ORDER BY total_wickets DESC
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        return df, series_name

    df["Economy"] = (df["runs"] / df["overs"]).round(2)

    df = df.rename(columns={
        "bowler": "Bowler",
        "venue": "Venue",
        "matches": "Matches",
        "total_wickets": "Total Wickets"
    })

    df = df[["Bowler", "Venue", "Matches", "Total Wickets", "Economy"]]

    return df, series_name


## Question 15 Identify players who perform exceptionally well in close matches.
## A close match is defined as one decided by 10 runs or fewer OR 2 wickets or fewer.


def get_que15_close_matches_performance():
   
    import re
    from sqlalchemy import text

    # ---------------- RESET TABLES ---------------- #

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS batting;"))
        conn.execute(text("DROP TABLE IF EXISTS matches;"))

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE matches (
                match_id BIGINT PRIMARY KEY,
                status TEXT
            );
        """))

        conn.execute(text("""
            CREATE TABLE batting (
                match_id BIGINT,
                player_name TEXT,
                team_name TEXT,
                runs INT,
                win_flag INT
            );
        """))

    # ---------------- FETCH RECENT MATCHES ---------------- #

    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"
    resp = requests.get(url, headers=headers)
    data = resp.json()

    match_records = []

    for type_block in data.get("typeMatches", []):
        for series in type_block.get("seriesMatches", []):

            wrapper = series.get("seriesAdWrapper")
            if not wrapper:
                continue

            for match in wrapper.get("matches", []):

                info = match.get("matchInfo", {})

                match_id = info.get("matchId")
                status = info.get("status")

                if match_id and status:
                    match_records.append({
                        "match_id": match_id,
                        "status": status
                    })

    matches_df = pd.DataFrame(match_records)

    if not matches_df.empty:
        matches_df.drop_duplicates(inplace=True)
        matches_df.to_sql("matches", engine, if_exists="append", index=False)

    # ---------------- IDENTIFY CLOSE MATCHES ---------------- #

    matches_df = pd.read_sql("SELECT * FROM matches", engine)

    close_ids = []

    for _, row in matches_df.iterrows():

        status = str(row["status"]).lower()

        r = re.search(r'won by (\d+) runs', status)
        w = re.search(r'won by (\d+) (?:wicket|wkts)', status)

        if (r and int(r.group(1)) <= 10) or (w and int(w.group(1)) <= 2):
            close_ids.append(row["match_id"])

    # ---------------- FETCH SCORECARDS ---------------- #

    batting_rows = []

    for mid in close_ids:

        url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{mid}/scard"
        resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            continue

        score = resp.json()

        match_status = score.get("status", "")

        winner = None
        if "won" in match_status.lower():
            winner = match_status.split(" won")[0].strip().lower()

        for innings in score.get("scorecard", []):

            bat_team = str(innings.get("batteamname", "")).lower()
            win_flag = 1 if winner and winner in bat_team else 0

            batsmen = innings.get("batsman", [])

            for b in batsmen:

                runs = b.get("runs")

                try:
                    runs = int(runs)
                except:
                    runs = 0

                batting_rows.append({
                    "match_id": mid,
                    "player_name": b.get("name"),
                    "team_name": innings.get("batteamname"),
                    "runs": runs,
                    "win_flag": win_flag
                })

    batting_df = pd.DataFrame(batting_rows)

    if not batting_df.empty:
        batting_df.to_sql("batting", engine, if_exists="append", index=False)

    # ---------------- FINAL QUERY ---------------- #

    query = """
    SELECT
        player_name AS player,
        team_name AS team,
        AVG(runs) AS avg_runs,
        COUNT(DISTINCT match_id) AS close_matches_played,
        SUM(win_flag) AS wins_when_batted
    FROM batting
    GROUP BY player_name, team_name
    ORDER BY avg_runs DESC
    LIMIT 10;
    """

    result = pd.read_sql(query, engine)

    return result



## Question 16 Track how players' batting performance changes over different years. 
## For matches since 2020, show each player's average runs per match and average strike rate for each year. 
## Only include players who played at least 5 matches in that year.


def get_que16_player_yearly_stats():

    import pandas as pd
    import requests
    import time
    from sqlalchemy import text

    # =========================
    # CREATE TABLE
    # =========================
    create_table = """
    CREATE TABLE IF NOT EXISTS q16_batting (
        player_id BIGINT,
        player_name TEXT,
        match_id BIGINT,
        match_year INT,
        runs INT,
        balls INT
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table))

    # =========================
    # CHECK EXISTING DATA
    # =========================
    count = pd.read_sql(
        "SELECT COUNT(*) FROM q16_batting",
        engine
    ).iloc[0, 0]

    if count == 0:

        print("Fetching Q16 raw data...")

        # GET 10 INDIA PLAYERS
        team_url = "https://cricbuzz-cricket.p.rapidapi.com/teams/v1/2/players"
        team_res = requests.get(team_url, headers=headers).json()

        players = []
        for p in team_res.get("player", []):
            if "id" in p:
                players.append(int(p["id"]))

        selected_players = set(players[:10])

        rows = []

        for year in range(2020, 2025):

            archive_url = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/archives/international"
            archive_res = requests.get(
                archive_url,
                headers=headers,
                params={"year": str(year)}
            ).json()

            india_series_ids = []

            for block in archive_res.get("seriesMapProto", []):
                for series in block.get("series", []):
                    if "India" in series.get("name", ""):
                        india_series_ids.append(series.get("id"))

            for sid in india_series_ids:

                series_url = f"https://cricbuzz-cricket.p.rapidapi.com/series/v1/{sid}"
                s_res = requests.get(series_url, headers=headers).json()

                for group in s_res.get("matchDetails", []):
                    match_map = group.get("matchDetailsMap", {})

                    for match in match_map.get("match", []):
                        mid = match.get("matchInfo", {}).get("matchId")

                        if not mid:
                            continue

                        score_url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{mid}/hscard"
                        score_res = requests.get(score_url, headers=headers).json()

                        for innings in score_res.get("scorecard", []):

                            if innings.get("batteamname", "").lower() != "india":
                                continue

                            for bat in innings.get("batsman", []):

                                pid = bat.get("id")

                                if pid in selected_players:

                                    runs = bat.get("runs")
                                    balls = bat.get("balls")
                                    name = bat.get("name")

                                    if runs is None or balls is None:
                                        continue

                                    rows.append({
                                        "player_id": pid,
                                        "player_name": name,
                                        "match_id": mid,
                                        "match_year": year,
                                        "runs": int(runs),
                                        "balls": int(balls)
                                    })

                        time.sleep(0.15)

        df_insert = pd.DataFrame(rows)

        if not df_insert.empty:
            df_insert.to_sql(
                "q16_batting",
                engine,
                if_exists="append",
                index=False
            )

    # =========================
    # SQL AGGREGATION
    # =========================
    query = """
    SELECT
        player_id,
        MIN(player_name) AS player_name,
        match_year,
        COUNT(DISTINCT match_id) AS matches,
        ROUND(AVG(runs)::numeric, 2) AS avg_runs_per_match,
        ROUND(
         ((SUM(runs)::numeric / NULLIF(SUM(balls),0)) * 100), 2
        ) AS strike_rate
    FROM q16_batting
    GROUP BY player_id, match_year
    HAVING COUNT(DISTINCT match_id) >= 5
    ORDER BY player_name, match_year;
    """

    result = pd.read_sql(query, engine)

    return result


## Question 17 Investigate whether winning the toss gives teams an advantage in winning matches. 
## Calculate what percentage of matches are won by the team that wins the toss,
## broken down by their toss decision (choosing to bat first or bowl first).

def get_q17_toss_advantage():

    from sqlalchemy import text
    import time

    # --- Create table ---
    create_table = """
    CREATE TABLE IF NOT EXISTS q17_raw (
        match_id BIGINT PRIMARY KEY,
        team1 TEXT,
        team2 TEXT,
        toss_winner TEXT,
        decision TEXT,
        match_winner TEXT
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table))

    count = pd.read_sql(
        "SELECT COUNT(*) FROM q17_raw",
        engine
    ).iloc[0, 0]

    if count == 0:

        url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"
        resp = requests.get(url, headers=headers)

        data = resp.json()

        match_ids = []

        for t in data.get("typeMatches", []):
            for series in t.get("seriesMatches", []):
                wrapper = series.get("seriesAdWrapper")
                if not wrapper:
                    continue

                for match in wrapper.get("matches", []):
                    info = match.get("matchInfo", {})
                    if info.get("state") == "Complete":
                        match_ids.append(info.get("matchId"))

        rows = []

        for mid in match_ids:

            base = requests.get(
                f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{mid}",
                headers=headers
            ).json()

            if base.get("state") != "Complete":
                continue

            toss = base.get("tossstatus")
            if not toss or " opt to " not in toss:
                continue

            toss_winner, decision = toss.split(" opt to ")
            toss_winner = toss_winner.strip().lower()
            decision = decision.strip().lower()

            scard = requests.get(
                f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{mid}/scard",
                headers=headers
            ).json()

            status = scard.get("status", "")

            if " won" in status:
                match_winner = status.split(" won")[0].strip().lower()
            else:
                continue

            rows.append({
                "match_id": mid,
                "team1": base.get("team1", {}).get("name", "").lower(),
                "team2": base.get("team2", {}).get("name", "").lower(),
                "toss_winner": toss_winner,
                "decision": decision,
                "match_winner": match_winner
            })

            time.sleep(0.2)

        df_insert = pd.DataFrame(rows)

        if not df_insert.empty:
            df_insert.to_sql(
                "q17_raw",
                engine,
                if_exists="append",
                index=False
            )

    summary_query = """
    WITH cleaned AS (
        SELECT *
        FROM q17_raw
        WHERE match_winner NOT LIKE '%tie%'
          AND match_winner NOT LIKE '%no result%'
    )
    SELECT
        decision,
        COUNT(*) AS total_matches,
        SUM(CASE WHEN toss_winner = match_winner THEN 1 ELSE 0 END) AS toss_won_match,
        ROUND(
            SUM(CASE WHEN toss_winner = match_winner THEN 1 ELSE 0 END)::numeric
            / COUNT(*) * 100,
            2
        ) AS toss_advantage_percentage
    FROM cleaned
    GROUP BY decision
    ORDER BY toss_advantage_percentage DESC;
    """

    return pd.read_sql(summary_query, engine)

## Question 18 Find the most economical bowlers in limited-overs cricket (ODI and T20 formats). 
## Calculate each bowler's overall economy rate and total wickets taken. 
## Only consider bowlers who have bowled in at least 10 matches and bowled at least 2 overs per match on average.

def get_que18_economical_bowlers():
    
    from sqlalchemy import text

    if engine is None:
        return pd.DataFrame([{"Error": "Database engine not available"}])

    # ---------- CREATE TABLE ----------
    create_table_query = text("""
    CREATE TABLE IF NOT EXISTS player_bowling_stats (
        player_id INT,
        player_name TEXT,
        team TEXT,
        format TEXT,
        matches INT,
        balls INT,
        runs INT,
        wickets INT
    );
    """)

    with engine.begin() as conn:
        conn.execute(create_table_query)
        conn.execute(text("DELETE FROM player_bowling_stats"))


    # ---------- API CALLS ----------
    def get_rankings(fmt):
        url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/bowlers?formatType={fmt}"
        res = requests.get(url, headers=headers).json()
        return res.get("rank", [])


    def extract_stats(data, fmt):

        headers_list = data.get("headers", [])
        values = data.get("values", [])

        if fmt not in headers_list:
            return 0, 0, 0, 0

        idx = headers_list.index(fmt)

        matches = balls = runs = wickets = 0

        for row in values:
            v = row.get("values", [])
            if not v:
                continue

            key = v[0].lower()

            try:
                val = float(str(v[idx]).replace("*", ""))
            except:
                val = 0

            if "match" in key:
                matches = int(val)
            elif "ball" in key:
                balls = int(val)
            elif "run" in key:
                runs = int(val)
            elif "wicket" in key:
                wickets = int(val)

        return matches, balls, runs, wickets


    # ---------- FETCH PLAYERS ----------
    players = {}

    for p in get_rankings("odi"):
        players[p["id"]] = {
            "name": p["name"],
            "team": p["country"]
        }

    for p in get_rankings("t20"):
        players[p["id"]] = {
            "name": p["name"],
            "team": p["country"]
        }


    insert_query = text("""
    INSERT INTO player_bowling_stats
    (player_id, player_name, team, format, matches, balls, runs, wickets)
    VALUES (:pid, :name, :team, :fmt, :mat, :balls, :runs, :wkts)
    """)


    # ---------- FETCH + INSERT ----------
    with engine.begin() as conn:

        for pid, info in players.items():

            name = info["name"]
            team = info["team"]

            try:
                url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{pid}/bowling"
                data = requests.get(url, headers=headers).json()

                odi = extract_stats(data, "ODI")
                t20 = extract_stats(data, "T20")

                if odi[0] > 0:
                    conn.execute(insert_query,[{
                        "pid": pid,
                        "name": name,
                        "team": team,
                        "fmt": "ODI",
                        "mat": odi[0],
                        "balls": odi[1],
                        "runs": odi[2],
                        "wkts": odi[3]
                    }])

                if t20[0] > 0:
                    conn.execute(insert_query,[{
                        "pid": pid,
                        "name": name,
                        "team": team,
                        "fmt": "T20",
                        "mat": t20[0],
                        "balls": t20[1],
                        "runs": t20[2],
                        "wkts": t20[3]
                    }])

                time.sleep(0.4)

            except Exception:
                continue


    # ---------- FINAL SQL QUERY ----------
    query = """
    SELECT
        ROW_NUMBER() OVER (ORDER BY economy ASC, wickets DESC) AS rank,
        bowler,
        team,
        matches,
        overs,
        wickets,
        economy
    FROM (
        SELECT
            player_name AS bowler,
            team,
            SUM(matches) AS matches,
            ROUND(SUM(balls)/6.0, 2) AS overs,
            SUM(wickets) AS wickets,
            ROUND(SUM(runs) / (SUM(balls)/6.0), 2) AS economy
        FROM player_bowling_stats
        WHERE format IN ('ODI','T20')
        GROUP BY player_name, team
        HAVING
            SUM(matches) >= 10
            AND (SUM(balls)/6.0) / SUM(matches) >= 2
    ) t
    ORDER BY rank;
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        return pd.DataFrame([{"Bowler": "No Data"}])

    return df


## Question 19 Determine which batsmen are most consistent in their scoring. 
## Calculate the average runs scored and the standard deviation of runs for each player. 
## Only include players who have faced at least 10 balls per innings and played since 2022.
## A lower standard deviation indicates more consistent performance.
def get_que19_player_consistency():

    import pandas as pd
    import requests
    import time
    from sqlalchemy import text

    # =========================
    # CREATE MATCH CACHE TABLE
    # =========================
    create_match_table = """
    CREATE TABLE IF NOT EXISTS q19_match_cache (
        match_id BIGINT PRIMARY KEY,
        match_year INT
    );
    """

    # =========================
    # CREATE BATTING TABLE
    # =========================
    create_batting_table = """
    CREATE TABLE IF NOT EXISTS q19_batting (
        player_id BIGINT,
        player_name TEXT,
        runs INT,
        balls INT,
        match_year INT
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_match_table))
        conn.execute(text(create_batting_table))

    # =========================
    # CHECK MATCH CACHE
    # =========================
    match_count = pd.read_sql(
        "SELECT COUNT(*) FROM q19_match_cache",
        engine
    ).iloc[0, 0]

    # =========================
    # FETCH MATCH IDS IF EMPTY
    # =========================
    if match_count == 0:

        print("Fetching match IDs...")

        all_rows = []

        for year in range(2022, 2027):

            url = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/archives/international"
            params = {"year": str(year)}

            response = requests.get(url, headers=headers, params=params)
            data = response.json()

            india_series_ids = []

            for block in data.get("seriesMapProto", []):
                for series in block.get("series", []):
                    if "India" in series.get("name", ""):
                        india_series_ids.append(series.get("id"))

            year_matches = []

            for sid in india_series_ids:

                s_url = f"https://cricbuzz-cricket.p.rapidapi.com/series/v1/{sid}"
                s_resp = requests.get(s_url, headers=headers)
                s_data = s_resp.json()

                for block in s_data.get("matchDetails", []):

                    match_map = block.get("matchDetailsMap", {})

                    for m in match_map.get("match", []):

                        mid = m.get("matchInfo", {}).get("matchId")

                        if mid:
                            year_matches.append((mid, year))

                        if len(year_matches) >= 3:
                            break

                    if len(year_matches) >= 3:
                        break

                if len(year_matches) >= 3:
                    break

            all_rows.extend(year_matches)

        df_match = pd.DataFrame(all_rows, columns=["match_id", "match_year"])

        if not df_match.empty:
            df_match.to_sql(
                "q19_match_cache",
                engine,
                if_exists="append",
                index=False
            )

    # =========================
    # CHECK BATTING TABLE
    # =========================
    batting_count = pd.read_sql(
        "SELECT COUNT(*) FROM q19_batting",
        engine
    ).iloc[0, 0]

    # =========================
    # FETCH BATTING IF EMPTY
    # =========================
    if batting_count == 0:

        print("Fetching batting data...")

        match_df = pd.read_sql(
            "SELECT match_id, match_year FROM q19_match_cache",
            engine
        )

        rows = []

        for _, row in match_df.iterrows():

            match_id = row["match_id"]
            year = row["match_year"]

            url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}/hscard"

            response = requests.get(url, headers=headers)
            data = response.json()

            for innings in data.get("scorecard", []):

                if innings.get("batteamname", "").lower() != "india":
                    continue

                for bat in innings.get("batsman", []):

                    pid = bat.get("id")
                    name = bat.get("name")
                    runs = bat.get("runs")
                    balls = bat.get("balls")

                    if pid is None or runs is None or balls is None:
                        continue

                    runs = int(runs)
                    balls = int(balls)

                    if balls < 10:
                        continue

                    rows.append({
                        "player_id": pid,
                        "player_name": name,
                        "runs": runs,
                        "balls": balls,
                        "match_year": year
                    })

            time.sleep(0.2)

        df_insert = pd.DataFrame(rows)

        if not df_insert.empty:
            df_insert.to_sql(
                "q19_batting",
                engine,
                if_exists="append",
                index=False
            )

    # =========================
    # SQL CONSISTENCY QUERY
    # =========================
    query = """
    WITH player_years AS (
        SELECT
            player_id,
            MIN(player_name) AS player_name,
            COUNT(DISTINCT match_year) AS years_played
        FROM q19_batting
        GROUP BY player_id
        HAVING COUNT(DISTINCT match_year) >= 3
    ),
    stats AS (
        SELECT
            b.player_id,
            p.player_name,
            COUNT(*) AS matches,
            AVG(b.runs) AS avg_runs,
            STDDEV(b.runs) AS std_dev
        FROM q19_batting b
        JOIN player_years p
            ON b.player_id = p.player_id
        GROUP BY b.player_id, p.player_name
    )
    SELECT *,
           std_dev / NULLIF(avg_runs,0) AS cv
    FROM stats
    ORDER BY cv ASC, matches DESC
    LIMIT 10;
    """

    result = pd.read_sql(query, engine)

    if not result.empty:
        result["avg_runs"] = result["avg_runs"].round(2)
        result["std_dev"] = result["std_dev"].round(2)
        result["cv"] = result["cv"].round(3)

    return result



## Question 20 Analyze how many matches each player has played in different cricket formats and their batting average in each format. 
## Show the count of Test matches, ODI matches, and T20 matches for each player, along with their respective batting averages.
## Only include players who have played at least 20 total matches across all formats.


def _build_que_20_information():

    team_url = "https://cricbuzz-cricket.p.rapidapi.com/teams/v1/2/players"
    team_res = safe_api_call(team_url, headers)

    if not team_res:
        return

    player_ids = []
    player_map = {}

    for p in team_res.get("player", []):
        if "id" in p:
            pid = int(p["id"])
            player_ids.append(pid)
            player_map[pid] = p["name"]

   
    player_ids = player_ids[:10]

    records = []

    for pid in player_ids:

        batting_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{pid}/batting"
        batting_res = safe_api_call(batting_url, headers)

        if not batting_res:
            continue

        headers_list = batting_res.get("headers", [])
        values_list = batting_res.get("values", [])

        if not headers_list or not values_list:
            continue

        formats = ["Test", "ODI", "T20"]

        for fmt in formats:

            if fmt not in headers_list:
                continue

            idx = headers_list.index(fmt)

            matches = 0
            average = 0

            for row in values_list:

                row_vals = row.get("values", [])
                if not row_vals:
                    continue

                stat_name = row_vals[0]
                stat_value = row_vals[idx] if len(row_vals) > idx else "0"

                try:
                    stat_value = float(str(stat_value).replace("*", ""))
                except:
                    stat_value = 0

                if stat_name == "Matches":
                    matches = int(stat_value)
                elif stat_name == "Average":
                    average = float(stat_value)

            records.append({
                "player_id": pid,
                "player_name": player_map[pid],
                "format": fmt,
                "matches": matches,
                "average": average
            })

    df_raw = pd.DataFrame(records)

    if df_raw.empty:
        return

    with engine.begin() as conn:
        df_raw.to_sql(
            "que_20_information",
            conn,
            if_exists="replace",
            index=False
        )
# =====================================================
# Question 20 — SQL ANALYTICS
# =====================================================
def get_que20_player_format_analysis():

    # Ensure table exists
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1 FROM que_20_information LIMIT 1"))
    except Exception:
        _build_que_20_information()

    query = """
        SELECT
            player_name,

            SUM(CASE WHEN format = 'Test' THEN matches ELSE 0 END) AS test_matches,
            SUM(CASE WHEN format = 'ODI' THEN matches ELSE 0 END) AS odi_matches,
            SUM(CASE WHEN format = 'T20' THEN matches ELSE 0 END) AS t20_matches,

            MAX(CASE WHEN format = 'Test' THEN average ELSE 0 END) AS test_avg,
            MAX(CASE WHEN format = 'ODI' THEN average ELSE 0 END) AS odi_avg,
            MAX(CASE WHEN format = 'T20' THEN average ELSE 0 END) AS t20_avg,

            SUM(matches) AS total_matches

        FROM que_20_information
        GROUP BY player_name
        HAVING SUM(matches) >= 20
        ORDER BY total_matches DESC;
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()

    df = pd.DataFrame(rows, columns=[
        "Player",
        "Test Matches", "ODI Matches", "T20 Matches",
        "Test Avg", "ODI Avg", "T20 Avg",
        "Total Matches"
    ])

    return df



#---------------------------------------------------------------------------------------------------------------------------------

## Question 21 Create a comprehensive performance ranking system for players.
## Combine their batting performance (runs scored, batting average, strike rate), bowling performance (wickets taken, bowling average, economy rate),
## and fielding performance (catches, stumpings, Run out) into a single weighted score. Use this formula and rank players:
## •	Batting points: (runs_scored × 0.01) + (batting_average × 0.5) + (strike_rate × 0.3)
## •	Bowling points: (wickets_taken × 2) + (50 - bowling_average) × 0.5) + ((6 - economy_rate) × 2)

## Rank the top performers in each cricket format.

import pandas as pd
import time
from sqlalchemy import text


def get_q21_composite_ranking():

    # ---------------------------------------
    # STEP 1 — FETCH INDIA PLAYERS
    # ---------------------------------------
    team_url = "https://cricbuzz-cricket.p.rapidapi.com/teams/v1/2/players"
    team_data = safe_api_call(team_url, headers)

    if not team_data:
        print("No team data")
        return pd.DataFrame()

    players = team_data.get("player", [])

    formats = ["Test", "ODI", "T20"]

    records = []

    # ---------------------------------------
    # STEP 2 — COLLECT PLAYER STATS
    # ---------------------------------------
    for p in players[:6]:   # limit players to avoid heavy API calls

        pid = p.get("id")
        pname = p.get("name")

        bat_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{pid}/batting"
        bowl_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{pid}/bowling"

        bat = safe_api_call(bat_url, headers)
        bowl = safe_api_call(bowl_url, headers)

        if not bat:
            continue

        for fmt in formats:

            if fmt not in bat.get("headers", []):
                continue

            idx = bat["headers"].index(fmt)

            runs = avg = sr = 0

            for row in bat.get("values", []):

                label = row["values"][0]
                val = row["values"][idx]

                if label == "Runs":
                    runs = int(val) if val.isdigit() else 0

                if label == "Average":
                    avg = float(val) if val.replace('.', '', 1).isdigit() else 0

                if label == "SR":
                    sr = float(val) if val.replace('.', '', 1).isdigit() else 0


            wickets = bowl_avg = eco = 0

            if bowl and fmt in bowl.get("headers", []):

                idx2 = bowl["headers"].index(fmt)

                for row in bowl.get("values", []):

                    label = row["values"][0]
                    val = row["values"][idx2]

                    if label == "Wickets":
                        wickets = int(val) if val.isdigit() else 0

                    if label == "Avg":
                        bowl_avg = float(val) if val.replace('.', '', 1).isdigit() else 0

                    if label == "Eco":
                        eco = float(val) if val.replace('.', '', 1).isdigit() else 0


            records.append({

                "player_name": pname,
                "format": fmt,
                "runs": runs,
                "batting_avg": avg,
                "strike_rate": sr,
                "wickets": wickets,
                "bowling_avg": bowl_avg,
                "economy": eco,
                "fielding_points": 0
            })

            time.sleep(0.05)


    if not records:
        return pd.DataFrame()

    df_raw = pd.DataFrame(records)

    # ---------------------------------------
    # STEP 3 — STORE IN DATABASE
    # ---------------------------------------
    with engine.begin() as conn:

        df_raw.to_sql(
            "que_21_information",
            conn,
            if_exists="replace",
            index=False
        )


    # ---------------------------------------
    # STEP 4 — FIELDING EXTRACTION 
    # ---------------------------------------
    match_id = 100337

    url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}/scard"

    data = safe_api_call(url, headers)

    if data:

        counts = {}

        for innings in data.get("scorecard", []):

            for batsman in innings.get("batsman", []):

                out_text = batsman.get("outdec", "")
                lower = out_text.lower()

                if lower.startswith("c "):

                    name = out_text.split(" b ")[0].replace("c ", "").strip()
                    counts[name] = counts.get(name, 0) + 1

                elif "run out" in lower:

                    inside = out_text.split("(")[-1].replace(")", "")
                    names = [n.strip() for n in inside.split("/")]

                    for n in names:
                        counts[n] = counts.get(n, 0) + 1


        with engine.begin() as conn:

            for player, pts in counts.items():

                conn.execute(text("""
                    UPDATE que_21_information
                    SET fielding_points = fielding_points + :pts
                    WHERE player_name = :player
                """), {"pts": pts, "player": player})


    # ---------------------------------------
    # STEP 5 — SQL ANALYTICS RANKING
    # ---------------------------------------
    query = """

    SELECT
        player_name,
        format,
        runs,
        batting_avg,
        strike_rate,
        wickets,
        bowling_avg,
        economy,
        fielding_points,

        (runs*0.01 + batting_avg*0.5 + strike_rate*0.3) AS batting_points,

        (wickets*2 + (50-bowling_avg)*0.5 + (6-economy)*2) AS bowling_points,

        (
            (runs*0.01 + batting_avg*0.5 + strike_rate*0.3)
            +
            (wickets*2 + (50-bowling_avg)*0.5 + (6-economy)*2)
            +
            fielding_points
        ) AS total_score,

        RANK() OVER(
            PARTITION BY format
            ORDER BY
            (
                (runs*0.01 + batting_avg*0.5 + strike_rate*0.3)
                +
                (wickets*2 + (50-bowling_avg)*0.5 + (6-economy)*2)
                +
                fielding_points
            ) DESC
        ) AS rank

    FROM que_21_information

    ORDER BY format, rank

    """


    with engine.connect() as conn:
       result = conn.execute(text(query))
       rows = result.fetchall()
       final_df = pd.DataFrame(rows, columns=result.keys())

    return final_df


## Question 22 Build a head-to-head match prediction analysis between teams. For each pair of teams that have played at least 5 matches against each other in the last 3 years, calculate:
## •	Total matches played between them
## •	Wins for each team
## •	Average victory margin when each team wins
## •	Performance when batting first vs bowling first at different venues
## •	Overall win percentage for each team in this head-to-head record


def get_q22_head_to_head_analysis():

    import pandas as pd
    import json
    import re

    # ------------------------------------------------
    # LOAD CACHED FILES
    # ------------------------------------------------

    df_matches = pd.read_csv("data/q22_ind_aus_matches.csv")

    with open("data/q22_match_cache.json") as f:
        winner_cache = json.load(f)

    with open("data/q22_margin_cache.json") as f:
        margin_cache = json.load(f)

    # ------------------------------------------------
    # EXTRACT WINNER
    # ------------------------------------------------

    winner_records = []

    for mid, data in winner_cache.items():

        short_status = data.get("shortstatus","")

        if "IND" in short_status:
            winner = "India"

        elif "AUS" in short_status:
            winner = "Australia"

        else:
            winner = None

        winner_records.append({
            "match_id": int(mid),
            "winner": winner
        })

    df_winner = pd.DataFrame(winner_records)

    # ------------------------------------------------
    # EXTRACT VICTORY MARGIN
    # ------------------------------------------------

    margin_records = []

    for mid, data in margin_cache.items():

        status = data.get("status","")

        margin_value = None
        margin_type = None

        m = re.search(r"won by (\d+) (runs|wkts|wickets)", status)

        if m:

            margin_value = int(m.group(1))

            if m.group(2) in ["wkts","wickets"]:
                margin_type = "wickets"
            else:
                margin_type = "runs"

        margin_records.append({
            "match_id": int(mid),
            "margin_value": margin_value,
            "margin_type": margin_type
        })

    df_margin = pd.DataFrame(margin_records)

    # ------------------------------------------------
    # MERGE DATASETS
    # ------------------------------------------------

    df_matches["match_id"] = df_matches["match_id"].astype(int)

    df = df_matches.merge(df_winner, on="match_id")
    df = df.merge(df_margin, on="match_id")

   
    df = df[df["winner"].notna()]

    # ------------------------------------------------
    # BAT FIRST CALCULATION
    # ------------------------------------------------

    df["bat_first"] = df["toss"].str.split(" opt").str[0]

    df["strategy"] = df.apply(
        lambda r: "bat_first" if r["winner"] == r["bat_first"] else "bowl_first",
        axis=1
    )

    # ------------------------------------------------
    # BASIC METRICS
    # ------------------------------------------------

    total_matches = len(df)

    wins = df["winner"].value_counts()

    india_wins = wins.get("India",0)
    aus_wins = wins.get("Australia",0)

    india_pct = round((india_wins / total_matches)*100,2)
    aus_pct = round((aus_wins / total_matches)*100,2)

    # ------------------------------------------------
    # STRATEGY PERFORMANCE
    # ------------------------------------------------

    strategy_perf = df.groupby(["winner","strategy"]).size().unstack(fill_value=0)

    india_bat_first = strategy_perf.loc["India"].get("bat_first",0)
    india_bowl_first = strategy_perf.loc["India"].get("bowl_first",0)

    aus_bat_first = strategy_perf.loc["Australia"].get("bat_first",0)
    aus_bowl_first = strategy_perf.loc["Australia"].get("bowl_first",0)

    # ------------------------------------------------
    # AVERAGE MARGINS
    # ------------------------------------------------

    runs_avg = df[df["margin_type"]=="runs"].groupby("winner")["margin_value"].mean()
    wkts_avg = df[df["margin_type"]=="wickets"].groupby("winner")["margin_value"].mean()

    india_runs_avg = round(runs_avg.get("India",0),2)
    aus_runs_avg = round(runs_avg.get("Australia",0),2)

    india_wkts_avg = round(wkts_avg.get("India",0),2)
    aus_wkts_avg = round(wkts_avg.get("Australia",0),2)

    # ------------------------------------------------
    # FINAL RESULT TABLE
    # ------------------------------------------------

    result = pd.DataFrame({

        "Metric":[
            "Total Matches Played",
            "Total Wins",
            "Avg Win Margin (Runs)",
            "Avg Win Margin (Wickets)",
            "Performance Batting First",
            "Performance Bowling First",
            "Overall Win Percentage"
        ],

        "India":[
            total_matches,
            india_wins,
            india_runs_avg,
            india_wkts_avg,
            india_bat_first,
            india_bowl_first,
            india_pct
        ],

        "Australia":[
            total_matches,
            aus_wins,
            aus_runs_avg,
            aus_wkts_avg,
            aus_bat_first,
            aus_bowl_first,
            aus_pct
        ]

    })

    # ------------------------------------------------
    # STORE IN DATABASE
    # ------------------------------------------------

    result.to_sql(
        "que_22_information",
        engine,
        if_exists="replace",
        index=False
    )

    return result

## Question 23 Analyze recent player form and momentum. For each player's last 10 batting performances, calculate:
## •	Average runs in their last 5 matches vs their last 10 matches
## •	Recent strike rate trends
## •	Number of scores above 50 in recent matches
## •	A consistency score based on standard deviation
## Based on these metrics, categorize players as being in "Excellent Form", "Good Form", "Average Form", or "Poor Form".


import os
import json
import pandas as pd
import numpy as np
from sqlalchemy import text


def get_q23_player_form_analysis(data_folder="data"):

    batting_records = []

    # -----------------------------
    # STEP 1 — LOAD JSON FILES
    # -----------------------------
    data_path = os.path.join(os.path.dirname(__file__), data_folder)

    for file in os.listdir(data_path):
        if file.startswith("match_") and file.endswith(".json"):
            match_id = file.split("_")[1].split(".")[0]
            with open(os.path.join(data_path, file), "r") as f:
               match_data = json.load(f)

            for innings in match_data.get("scorecard", []):

                for batsman in innings.get("batsman", []):

                    batting_records.append({
                        "match_id": int(match_id),
                        "player": batsman.get("name"),
                        "runs": int(batsman.get("runs", 0)),
                        "balls": int(batsman.get("balls", 0)),
                        "strike_rate": float(batsman.get("strkrate", 0))
                    })

    df_raw = pd.DataFrame(batting_records)

    if df_raw.empty:
        return df_raw

    # -----------------------------
    # STEP 2 — SORT BY PLAYER + MATCH
    # -----------------------------
    df_raw = df_raw.sort_values(
        by=["player", "match_id"],
        ascending=[True, False]
    )

    # -----------------------------
    # STEP 3 — LAST 10 INNINGS
    # -----------------------------
    df_last10 = df_raw.groupby("player").head(10)

    player_counts = df_last10["player"].value_counts()

    eligible_players = player_counts[player_counts >= 10].index

    df_last10 = df_last10[df_last10["player"].isin(eligible_players)]

    # -----------------------------
    # STEP 4 — STORE RAW DATA
    # -----------------------------
    with engine.begin() as conn:

        df_last10.to_sql(
            "que_23_player_form",
            conn,
            if_exists="replace",
            index=False
        )

    # -----------------------------
    # STEP 5 — SQL ANALYTICS
    # -----------------------------
    query = """

WITH last10 AS (

    SELECT
        player,
        runs,
        strike_rate,
        match_id,

        ROW_NUMBER() OVER(
            PARTITION BY player
            ORDER BY match_id DESC
        ) AS rn

    FROM que_23_player_form
),

metrics AS (

    SELECT
        player,

        AVG(CASE WHEN rn <= 5 THEN runs END) AS avg_last5,
        AVG(runs) AS avg_last10,

        AVG(CASE WHEN rn <= 5 THEN strike_rate END) AS sr_last5,
        AVG(strike_rate) AS sr_last10,

        SUM(CASE WHEN runs >= 50 THEN 1 ELSE 0 END) AS fifties,

        STDDEV(runs) AS std_dev

    FROM last10

    GROUP BY player
)

SELECT
    player,

    ROUND(avg_last5::numeric,2) AS avg_last5,
    ROUND(avg_last10::numeric,2) AS avg_last10,

    ROUND(sr_last5::numeric,2) AS sr_last5,
    ROUND(sr_last10::numeric,2) AS sr_last10,

    fifties,

    ROUND(std_dev::numeric,2) AS std_dev,

    ROUND((100/(1+std_dev))::numeric,2) AS consistency_score,

    CASE
        WHEN avg_last5 > avg_last10
            AND fifties >= 3
            AND std_dev < 25
        THEN 'Excellent Form'

        WHEN avg_last5 >= avg_last10
            AND fifties >= 2
        THEN 'Good Form'

        WHEN avg_last5 < avg_last10
            AND std_dev > 35
        THEN 'Poor Form'

        ELSE 'Average Form'
    END AS form_category

FROM metrics

ORDER BY avg_last5 DESC

    """

    with engine.connect() as conn:

        result = conn.execute(text(query))
        rows = result.fetchall()

    if not rows:
        return pd.DataFrame()

    df_result = pd.DataFrame(rows, columns=result.keys())

    return df_result


## Question 24 Study successful batting partnerships to identify the best player combinations.
## For pairs of players who have batted together as consecutive batsmen (positions differ by 1) in at least 5 partnerships:
## •	Calculate their average partnership runs
## •	Count how many of their partnerships exceeded 50 runs
## •	Find their highest partnership score
## •	Calculate their success rate (percentage of good partnerships)
## Rank the most successful batting partnerships.

import os
import json
import pandas as pd
from sqlalchemy import text


def get_q24_batting_partnerships(data_folder="data"):

    batting_records = []

    data_path = os.path.join(os.path.dirname(__file__), data_folder)

    # -----------------------------
    # STEP 1 — LOAD JSON
    # -----------------------------
    for file in os.listdir(data_path):

        if file.startswith("match_") and file.endswith(".json"):

            match_id = file.split("_")[1].split(".")[0]

            with open(os.path.join(data_path, file), "r") as f:
                match_data = json.load(f)

            for innings in match_data.get("scorecard", []):

                innings_id = innings.get("inningsid")

                position = 1

                for batsman in innings.get("batsman", []):

                    batting_records.append({
                        "match_id": int(match_id),
                        "innings": innings_id,
                        "player": batsman.get("name"),
                        "runs": int(batsman.get("runs", 0)),
                        "position": position
                    })

                    position += 1


    df_raw = pd.DataFrame(batting_records)

    if df_raw.empty:
        return df_raw


    # -----------------------------
    # STEP 2 — STORE RAW DATA
    # -----------------------------
    with engine.begin() as conn:

        df_raw.to_sql(
            "que_24_batting_positions",
            conn,
            if_exists="replace",
            index=False
        )


    # -----------------------------
    # STEP 3 — SQL PARTNERSHIP ANALYSIS
    # -----------------------------
    query = """

    WITH ordered AS (

        SELECT
            match_id,
            innings,
            player,
            runs,
            position,

            LEAD(player) OVER(
                PARTITION BY match_id, innings
                ORDER BY position
            ) AS next_player,

            LEAD(runs) OVER(
                PARTITION BY match_id, innings
                ORDER BY position
            ) AS next_runs

        FROM que_24_batting_positions
    ),

    partnerships AS (

        SELECT
            player AS player1,
            next_player AS player2,
            runs + next_runs AS partnership_runs

        FROM ordered

        WHERE next_player IS NOT NULL
    ),

    summary AS (

        SELECT
            player1,
            player2,

            COUNT(*) AS total_partnerships,

            AVG(partnership_runs) AS avg_partnership,

            SUM(
                CASE
                    WHEN partnership_runs >= 50
                    THEN 1 ELSE 0
                END
            ) AS fifty_plus,

            MAX(partnership_runs) AS highest_partnership,

            100.0 * SUM(
                CASE
                    WHEN partnership_runs >= 50
                    THEN 1 ELSE 0
                END
            ) / COUNT(*) AS success_rate

        FROM partnerships

        GROUP BY player1, player2

        HAVING COUNT(*) >= 5
    )

    SELECT
        player1,
        player2,
        total_partnerships,
        ROUND(avg_partnership::numeric,2) AS avg_partnership,
        fifty_plus,
        highest_partnership,
        ROUND(success_rate::numeric,2) AS success_rate,

        RANK() OVER(
            ORDER BY success_rate DESC, avg_partnership DESC
        ) AS rank

    FROM summary

    ORDER BY rank
    """

    with engine.connect() as conn:

        result = conn.execute(text(query))
        rows = result.fetchall()

    if not rows:
        return pd.DataFrame()

    df_result = pd.DataFrame(rows, columns=result.keys())

    return df_result

## Question 25 Perform a time-series analysis of player performance evolution. 
## Track how each player's batting performance changes over time by:
## •	Calculating quarterly averages for runs and strike rate
## •	Comparing each quarter's performance to the previous quarter
## •	Identifying whether performance is improving, declining, or stable
## •	Determining overall career trajectory over the last few years
## •	Categorizing players' career phase as "Career Ascending", "Career Declining", or "Career Stable"
## Only analyze players with data spanning at least 6 quarters and a minimum of 3 matches per quarter.
from sqlalchemy import text
import os
import json
import pandas as pd


def get_q25_player_time_series(data_folder="data"):

    batting_records = []

    data_path = os.path.join(os.path.dirname(__file__), data_folder)

    # ---------------------------------------
    # STEP 1 — LOAD JSON FILES
    # ---------------------------------------
    for file in os.listdir(data_path):

        if not file.startswith("match_") or not file.endswith(".json"):
            continue

        match_id_part = file.split("_")[1].split(".")[0]

        if not match_id_part.isdigit():
            continue

        match_id = int(match_id_part)

        file_path = os.path.join(data_path, file)

        try:
            with open(file_path, "r") as f:
                match_data = json.load(f)
        except:
            continue

        for innings in match_data.get("scorecard", []):

            for batsman in innings.get("batsman", []):

                batting_records.append({

                    "match_id": match_id,
                    "player": batsman.get("name"),
                    "runs": int(batsman.get("runs", 0)),
                    "strike_rate": float(batsman.get("strkrate", 0))

                })

    df_raw = pd.DataFrame(batting_records)

    if df_raw.empty:
        return pd.DataFrame()

    # ---------------------------------------
    # STEP 2 — STORE RAW DATA
    # ---------------------------------------
    with engine.begin() as conn:

        df_raw.to_sql(
            "que_25_player_timeseries",
            conn,
            if_exists="replace",
            index=False
        )

    # ---------------------------------------
    # STEP 3 — SQL TIME SERIES ANALYSIS
    # ---------------------------------------
    query = """

  WITH quarterly AS (

SELECT
    player,
    match_id AS quarter,

    AVG(runs) AS avg_runs,
    AVG(strike_rate) AS avg_sr,
    COUNT(*) AS matches

FROM que_25_player_timeseries

GROUP BY player, match_id

),

filtered_quarters AS (

SELECT *
FROM quarterly
WHERE matches >= 1

),

valid_players AS (

SELECT
    player
FROM filtered_quarters
GROUP BY player
HAVING COUNT(*) >= 3

),

trends AS (

SELECT
    fq.player,
    fq.quarter,
    fq.avg_runs,
    fq.avg_sr,
    fq.matches,

    LAG(fq.avg_runs) OVER(
        PARTITION BY fq.player
        ORDER BY fq.quarter
    ) AS prev_runs

FROM filtered_quarters fq

JOIN valid_players vp
ON fq.player = vp.player

),

trend_class AS (

SELECT
    player,
    quarter,
    avg_runs,
    avg_sr,
    matches,

    CASE
        WHEN prev_runs IS NULL THEN 'Stable'
        WHEN avg_runs > prev_runs THEN 'Improving'
        WHEN avg_runs < prev_runs THEN 'Declining'
        ELSE 'Stable'
    END AS trend

FROM trends

),

career_phase AS (

SELECT
    player,

    SUM(CASE WHEN trend='Improving' THEN 1 ELSE 0 END) AS improving,
    SUM(CASE WHEN trend='Declining' THEN 1 ELSE 0 END) AS declining

FROM trend_class

GROUP BY player

)

SELECT
    t.player,
    t.quarter AS match_order,

    ROUND(t.avg_runs::numeric,2) AS avg_runs,
    ROUND(t.avg_sr::numeric,2) AS avg_strike_rate,

    t.matches,
    t.trend,

    CASE
        WHEN c.improving > c.declining THEN 'Career Ascending'
        WHEN c.declining > c.improving THEN 'Career Declining'
        ELSE 'Career Stable'
    END AS career_phase

FROM trend_class t

JOIN career_phase c
ON t.player = c.player

ORDER BY t.player, t.quarter

    """

    with engine.connect() as conn:

        result = conn.execute(text(query))
        rows = result.fetchall()

    if not rows:
        return pd.DataFrame()

    df_result = pd.DataFrame(rows, columns=result.keys())

    return df_result









