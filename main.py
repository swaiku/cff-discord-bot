import requests
from google.transit import gtfs_realtime_pb2
import pandas as pd
from datetime import datetime
import zipfile
import os
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Load configuration
with open("config.json", "r") as config_file:
    config = json.load(config_file)

LINE_NAME = config["LINE_NAME"]
LINE_STOPS = config["LINE_STOPS"]

# Environment variables
GTFS_RT_URL = os.getenv("GTFS_RT_URL")
GTFS_STATIC_URL = os.getenv("GTFS_STATIC_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
API_KEY = os.getenv("TOKEN")
GTFS_DIR = "gtfs_data"  # Directory for GTFS files

def check_gtfs_up_to_date(gtfs_url, local_zip="gtfs_static.zip"):
    """Verify if the local GTFS files are up to date with the remote ones."""
    try:
        response = requests.head(gtfs_url, allow_redirects=True)
        response.raise_for_status()
        remote_size = int(response.headers.get("Content-Length", 0))

        if os.path.exists(local_zip):
            local_size = os.path.getsize(local_zip)
            if local_size == remote_size:
                print("✅ GTFS files are up to date.")
                return True
        return False
    except Exception as e:
        print(f"❌ Error checking GTFS files: {e}")
        return False

def download_and_extract_gtfs(gtfs_url, force_download=False):
    """Download and extract GTFS files if needed."""
    try:
        if not force_download and check_gtfs_up_to_date(gtfs_url):
            print("ℹ️ No download needed. Extracting existing files...")
            if os.path.exists("gtfs_static.zip"):
                with zipfile.ZipFile("gtfs_static.zip", "r") as zip_ref:
                    zip_ref.extractall(GTFS_DIR)
                print(f"✅ GTFS files extracted to {GTFS_DIR}.")
            return

        print(f"🔄 Downloading GTFS files from {gtfs_url}...")
        response = requests.get(gtfs_url)
        response.raise_for_status()

        with open("gtfs_static.zip", "wb") as f:
            f.write(response.content)

        with zipfile.ZipFile("gtfs_static.zip", "r") as zip_ref:
            zip_ref.extractall(GTFS_DIR)

        print(f"✅ GTFS files downloaded and extracted to {GTFS_DIR}.")
    except Exception as e:
        print(f"❌ Error downloading or extracting GTFS files: {e}")

def validate_gtfs_files(gtfs_dir):
    """Check if all necessary GTFS files are present."""
    required_files = ["trips.txt", "routes.txt", "stops.txt", "stop_times.txt"]
    return all(os.path.exists(os.path.join(gtfs_dir, file)) for file in required_files)

def get_line_trip_ids(gtfs_dir):
    """Fetch trip IDs for the specified line."""
    routes = pd.read_csv(os.path.join(gtfs_dir, "routes.txt"), usecols=["route_id", "route_short_name"], dtype=str)
    trips = pd.read_csv(os.path.join(gtfs_dir, "trips.txt"), usecols=["trip_id", "route_id"], dtype=str)

    line_route_ids = routes[routes["route_short_name"] == LINE_NAME]["route_id"]
    line_trips = trips[trips["route_id"].isin(line_route_ids)]
    return set(line_trips["trip_id"])

def normalize_stop_id(stop_id):
    """Normalize stop IDs to align GTFS-RT and static formats."""
    return stop_id.split(":")[0] if ":" in stop_id else stop_id

def filter_stops_and_times(gtfs_dir, line_trip_ids):
    """Filter stops and stop_times for the specified line."""
    stops_df = pd.read_csv(os.path.join(gtfs_dir, "stops.txt"), usecols=["stop_id", "stop_name"], dtype=str)
    stops_df["stop_id"] = stops_df["stop_id"].apply(normalize_stop_id)
    stops_df = stops_df[stops_df["stop_name"].isin(LINE_STOPS)]

    stop_times_df = pd.read_csv(
        os.path.join(gtfs_dir, "stop_times.txt"),
        usecols=["trip_id", "stop_id", "arrival_time", "departure_time"],
        dtype=str
    )
    stop_times_df["stop_id"] = stop_times_df["stop_id"].apply(normalize_stop_id)
    stop_times_df = stop_times_df[
        stop_times_df["trip_id"].isin(line_trip_ids) & stop_times_df["stop_id"].isin(stops_df["stop_id"])
    ]

    trip_start_end = stop_times_df.groupby("trip_id").agg(
        start_stop=pd.NamedAgg(column="stop_id", aggfunc="first"),
        end_stop=pd.NamedAgg(column="stop_id", aggfunc="last")
    ).reset_index()

    invalid_trips = trip_start_end[trip_start_end["start_stop"] == trip_start_end["end_stop"]]["trip_id"]
    stop_times_df = stop_times_df[~stop_times_df["trip_id"].isin(invalid_trips)]

    print(f"✅ Filtered {len(stops_df)} stops and {len(stop_times_df)} schedules for {LINE_NAME}.")
    return stops_df, stop_times_df

def fetch_realtime_data(line_trip_ids, stops_df, stop_times_df):
    """Fetch GTFS-RT data and combine it with static schedules."""
    headers = {"Authorization": f"Bearer {API_KEY}"}
    try:
        print("🔄 Fetching GTFS-RT data...")
        response = requests.get(GTFS_RT_URL, headers=headers)
        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        trip_updates = []
        for entity in feed.entity:
            if entity.trip_update:
                trip_id = entity.trip_update.trip.trip_id.split(":")[-1]
                for stop_time_update in entity.trip_update.stop_time_update:
                    stop_id = normalize_stop_id(stop_time_update.stop_id)
                    arrival_delay = stop_time_update.arrival.delay if stop_time_update.HasField("arrival") else 0
                    departure_delay = stop_time_update.departure.delay if stop_time_update.HasField("departure") else 0

                    trip_updates.append({
                        "trip_id": trip_id,
                        "stop_id": stop_id,
                        "arrival_delay": arrival_delay,
                        "departure_delay": departure_delay,
                    })

        realtime_df = pd.DataFrame(trip_updates)
        print("🛠 Diagnostic - Extracted trip_updates:")
        print(realtime_df.head())

        if realtime_df.empty:
            print(f"✅ No active {LINE_NAME} trips found.")
            print("⏰ Current schedule without delays:")
            print(stop_times_df.head())
            return

        merged_df = realtime_df.merge(stops_df, on="stop_id", how="left")
        merged_df = merged_df.merge(stop_times_df, on=["trip_id", "stop_id"], how="left")

        today = datetime.now().date()
        merged_df["arrival_time"] = pd.to_datetime(
            today.strftime("%Y-%m-%d") + " " + merged_df["arrival_time"],
            format="%Y-%m-%d %H:%M:%S",
            errors="coerce"
        )
        merged_df["departure_time"] = pd.to_datetime(
            today.strftime("%Y-%m-%d") + " " + merged_df["departure_time"],
            format="%Y-%m-%d %H:%M:%S",
            errors="coerce"
        )

        merged_df["real_arrival_time"] = merged_df["arrival_time"] + pd.to_timedelta(merged_df["arrival_delay"], unit="s")
        merged_df["real_departure_time"] = merged_df["departure_time"] + pd.to_timedelta(merged_df["departure_delay"], unit="s")

        merged_df = merged_df[merged_df["stop_name"].isin(LINE_STOPS)]

        delayed_trips = merged_df[merged_df["arrival_delay"] >= 120]["trip_id"].unique()
        if not delayed_trips.size:
            print(f"✅ No significant delays for the {LINE_NAME} line.")
            print("⏰ Current schedule without delays:")
            print(stop_times_df.head())
            return

        delayed_df = merged_df[merged_df["trip_id"].isin(delayed_trips)]
        delayed_df = delayed_df.sort_values(by=["trip_id", "real_arrival_time"]).drop_duplicates(
            subset=["trip_id", "stop_id"], keep="first"
        )

        print("Delayed trips detected:", delayed_trips)

        message = f"🚄 **Active {LINE_NAME} trips with delays**:\n"
        for trip_id, group in delayed_df.groupby("trip_id"):
            start = group.iloc[0]["stop_name"]
            end = group.iloc[-1]["stop_name"]
            message += f"\n**From {start} to {end}**\n"
            for _, row in group.iterrows():
                arrival = row['real_arrival_time'].strftime('%H:%M') if pd.notna(row['real_arrival_time']) else "N/A"
                message += f"- **{row['stop_name']}**: Arrival: {arrival}, Delay: +{row['arrival_delay'] // 60} min\n"

        send_discord_notification(message)
        print(message)

    except Exception as e:
        print(f"❌ Error fetching real-time data: {e}")
        send_discord_notification(f"❌ Error fetching real-time data: {e}")

def send_discord_notification(message):
    """Send a message to the Discord webhook."""
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json={"content": message})
        response.raise_for_status()
        print("✅ Notification sent.")
    except Exception as e:
        print(f"❌ Discord notification failed: {e}")

if __name__ == "__main__":
    if not GTFS_STATIC_URL:
        print("❌ GTFS_STATIC_URL is not set in the environment.")
    else:
        download_and_extract_gtfs(GTFS_STATIC_URL)

        if validate_gtfs_files(GTFS_DIR):
            line_trip_ids = get_line_trip_ids(GTFS_DIR)
            stops, stop_times = filter_stops_and_times(GTFS_DIR, line_trip_ids)
            fetch_realtime_data(line_trip_ids, stops, stop_times)
        else:
            print("❌ Required GTFS files are missing.")
