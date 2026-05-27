"""
Run live data ingestion pipeline.

This script hooks up the live data ingestor to a data feed.
By default, it can start the StreamIngestor and wait for webhook or API updates.
With the --simulate flag, it cleanly simulates a real-time data feed using 
historical Cricsheet data from a duckdb database, streaming deliveries one 
by one to test the pipeline.
"""

import argparse
import sys
import time
import logging
import random
from pathlib import Path
import pandas as pd

from midwicket.storage.engine import QueryEngine
from midwicket.live.ingestor import create_stream_ingestor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Run Live Data Ingestion")
    parser.add_argument("--db-path", required=True, help="Path to destination DuckDB database")
    parser.add_argument("--simulate", action="store_true", help="Simulate live feed using historical data")
    parser.add_argument("--source-db", help="Path to source DuckDB with historical ball_events (used for simulation). Defaults to --db-path if not set.")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay in seconds between deliveries during simulation")
    parser.add_argument("--continuous", action="store_true", help="Continuously loop through matches in simulation")
    parser.add_argument("--limit", type=int, default=None, help="Limit the number of matches to simulate")
    parser.add_argument("--match-id", type=str, default=None, help="Specific match ID to simulate")
    return parser.parse_args()

def simulate_live_feed(ingestor, source_engine: QueryEngine, delay: float, continuous: bool, limit: int = None, specific_match_id: str = None):
    """Simulates a live feed by reading from historical ball_events and streaming them."""
    if not source_engine.table_exists("ball_events"):
        logger.error("Source database does not have a 'ball_events' table.")
        return

    # Get list of unique match_ids
    if specific_match_id:
        match_ids = [specific_match_id]
    else:
        matches_df = source_engine.execute_sql("SELECT DISTINCT match_id FROM ball_events").to_pandas()
        match_ids = matches_df["match_id"].tolist()
        
    if not match_ids:
        logger.error("No matches found in source database.")
        return

    if limit:
        random.shuffle(match_ids)
        match_ids = match_ids[:limit]

    logger.info(f"Found {len(match_ids)} matches in source database for simulation.")

    while True:
        # Shuffle to simulate random live matches
        random.shuffle(match_ids)
        for match_id in match_ids:
            logger.info(f"Starting simulation for match: {match_id}")
            # Fetch all deliveries for this match
            query = """
                SELECT *
                FROM ball_events
                WHERE match_id = ?
                ORDER BY inning, over, ball
            """
            events_df = source_engine.execute_sql(query, params=[match_id]).to_pandas()
            
            # Create a fake live match ID to avoid collision if source == dest
            live_match_id = f"{match_id}_live_{int(time.time())}"
            ingestor.register_match(live_match_id, source="simulation")
            
            runs_total = 0
            wickets_fallen = 0
            current_inning = None
            
            for _, row in events_df.iterrows():
                delivery_data = row.to_dict()
                
                # Reset cumulative counts if new inning
                if delivery_data["inning"] != current_inning:
                    runs_total = 0
                    wickets_fallen = 0
                    current_inning = delivery_data["inning"]
                    
                runs = delivery_data.get("runs_batter", 0) + delivery_data.get("runs_extras", 0)
                is_wicket = bool(delivery_data.get("is_wicket", False))
                
                runs_total += runs
                if is_wicket:
                    wickets_fallen += 1
                    
                delivery_data["runs_total"] = runs_total
                delivery_data["wickets_fallen"] = wickets_fallen
                
                # Update match_id to the live one
                delivery_data["match_id"] = live_match_id
                
                # Replace pandas NaN with None for JSON/dict compatibility
                delivery_data = {k: (None if pd.isna(v) else v) for k, v in delivery_data.items()}
                
                # Enqueue the delivery to be processed by the ingestor
                ingestor.update_match_data(live_match_id, delivery_data)
                
                # Sleep to simulate real-time delay between balls
                time.sleep(delay)
                
            ingestor.unregister_match(live_match_id)
            logger.info(f"Completed simulation for match: {match_id}")
            
        if not continuous:
            break

def main():
    args = parse_args()
    
    db_path = Path(args.db_path)
    # Ensure parent dir exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    query_engine = QueryEngine(str(db_path))
    ingestor = create_stream_ingestor(query_engine)
    
    # Start the background threads and optionally webhook server
    ingestor.start()
    
    try:
        if args.simulate:
            source_db_path = args.source_db if args.source_db else args.db_path
            source_engine = QueryEngine(str(source_db_path)) if source_db_path != str(db_path) else query_engine
            logger.info(f"Starting simulation. Source DB: {source_db_path}, Dest DB: {db_path}")
            simulate_live_feed(ingestor, source_engine, args.delay, args.continuous, args.limit, args.match_id)
            if source_engine is not query_engine:
                source_engine.close()
        else:
            # Just run normally, waiting for webhooks or API polling
            logger.info("Ingestor running in background. Waiting for webhook/API data...")
            while True:
                time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
    except Exception as e:
        logger.error(f"Error in main loop: {e}", exc_info=True)
    finally:
        logger.info("Stopping ingestor...")
        ingestor.stop()
        query_engine.close()

if __name__ == "__main__":
    sys.exit(main())
