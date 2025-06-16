import pandas as pd
from datetime import datetime
import psycopg2
import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# EXTRACT
def extract(file_path):
    df = pd.read_csv(file_path)

    return df

# TRANSFORM
def transform(df):
    columns_to_clean = ["player_name", "club", "nationality", "league", "league_country"]

    for column in columns_to_clean:
        df[column] = df[column].astype(str).str.strip().str.title()


    df["signing_date"] = df["signing_date"].astype(str).str.strip()

    date_formats = [
        "%d-%m-%y",        # e.g., '04-13-20', '11-06-21'
        "%d-%m-%Y",        # e.g., '28-07-2015', '27-06-2024'
        "%Y.%m.%d",        # e.g., '2024.07.01', '2016.10.02'.                     the date format stuff is usually referred to as strftime / strptime format codes
        "%b %d, %Y",       # e.g., 'Jul 08, 2021', 'Apr 28, 2019'
        "%B %d, %Y",       # e.g., 'October 21, 2018', 'December 07, 2023'
        "%d/%m/%y",        # e.g., '12/11/20', '02/07/21'
        "%Y/%m/%d",        # e.g., '2023/03/09', '2016/09/19'
        "%d %B %Y",        # e.g., '06 March 2024', '20 March 2015'
        "%m-%d-%y",        # e.g., '08-29-17', '04-13-20'
        "%m/%d/%y",        # e.g., '08/01/17' (if any US-style exists)
        "%d/%m/%Y",        # e.g., '26/08/15', '24/09/18'
    ]
    def parse_date(date_str):
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date() #strptime means string --> parse --> time. it's a function that parses a string into a datetime.datetime object. we added date() at the end to get rid of the time part so the final output is just the date
            except ValueError:
                continue
        return pd.NaT  # Not a Time (missing/invalid)

    df["signing_date"] = df["signing_date"].apply(parse_date)


    # SPLIT DATA INTO 2 TABLES
    # Table 1: players - player_id, player_name, age, appearances, goals, assists, nationality, signing_date, club_id (need to create this)
    # Table 2: clubs - with club-related data (e.g., club_id (need to create this), club, league, league_country)


    # Create clubs table
    clubs_df = df[["club", "league", "league_country"]].drop_duplicates().reset_index(drop=True) # selects columns, drops duplicates, resets index after dropping duplicates
    clubs_df["club_id"] = clubs_df.index + 1 # creates a new column called club_id and assigns each row a unique value. .index gives numbers like 0,1,2,3... the +1 makes it start from 1 instead
    clubs_df = clubs_df[['club_id', 'club', 'league', 'league_country']] # Reorder columns

    # Merge clubs table back to main df
    df = df.merge(clubs_df, on=["club", "league", "league_country"], how="left")

    # Create player table
    players_df = df[["player_id", "player_name", "age", "appearances", "goals", "assists", "nationality", "signing_date", "club_id"]]

    return players_df, clubs_df

# LOAD
def load(players_df, clubs_df):
    load_dotenv()

    db_username = os.getenv("db_username")
    db_password = os.getenv("db_password")
    db_host = "localhost"
    db_port = 5432
    db_name = "Football_ETL"

    engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

    # Load both tables into PostgreSQL
    clubs_df.to_sql(
        name='clubs',
        con=engine,
        if_exists='replace',
        index=False
    )

    players_df.to_sql(
        name='players',
        con=engine,
        if_exists='replace',
        index=False
    )

def main(file_path):
    df = extract(file_path)
    players_df, clubs_df = transform(df)
    load(players_df, clubs_df)

if __name__ == "__main__":
    path = "/Users/amanpatel/Desktop/Data Engineering Projects/PostgreSQL Projects/Project 6 - Football ETL into 2 SQL tables/very_messy_football_data_24_25.csv"
    main(path)