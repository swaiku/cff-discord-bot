import os
import pandas as pd
import requests
import zipfile
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

GTFS_STATIC_URL = os.environ["GTFS_STATIC_URL"]
GTFS_RT_URL = os.environ["GTFS_RT_URL"]
TOKEN = os.environ["TOKEN"]
CFF_ID = os.environ["CFF_ID"]
LINE_NAME = os.environ["LINE_NAME"]
LOCAL_GTFS_DIR = "gtfs_static_data"
STOP_CONFIG_FILE = "stop_config.txt"

def ensure_local_gtfs():
    os.makedirs(LOCAL_GTFS_DIR, exist_ok=True)

    # Get remote file name
    response = requests.head(GTFS_STATIC_URL)
    response.raise_for_status()
    remote_zip_name = os.path.basename(response.headers.get('Location'))
    remote_zip_path = os.path.join(LOCAL_GTFS_DIR, remote_zip_name)

    # if the remote name is the same, keep it
    if os.path.exists(remote_zip_path):
        print("Le fichier GTFS statique est déjà à jour.")
        return remote_zip_path

    # else remove old files
    for file in os.listdir(LOCAL_GTFS_DIR):
        os.remove(os.path.join(LOCAL_GTFS_DIR, file))

    # download new file
    print("Téléchargement des données GTFS statiques...")
    response = requests.get(GTFS_STATIC_URL)
    response.raise_for_status()
    with open(remote_zip_path, 'wb') as f:
        f.write(response.content)

    # Extract files
    with zipfile.ZipFile(remote_zip_path, 'r') as z:
        z.extractall(LOCAL_GTFS_DIR)

    print("Mise à jour des données GTFS statiques terminée.")
    return remote_zip_path


def save_trips_related_to_stops():
    filtered_trips_path = os.path.join(LOCAL_GTFS_DIR, 'filtered_trips.csv')

    # Vérifier si le fichier existe déjà
    if os.path.exists(filtered_trips_path):
        print(f"Fichier filtré des trajets déjà existant : {filtered_trips_path}")
        return filtered_trips_path

    # Charger les arrêts de la configuration
    stop_config = pd.read_csv(STOP_CONFIG_FILE, header=None, names=['stop_id', 'stop_name'], dtype='str')
    target_stop_ids = set(stop_config['stop_id'])

    trips_path = os.path.join(LOCAL_GTFS_DIR, 'trips.txt')
    stop_times_path = os.path.join(LOCAL_GTFS_DIR, 'stop_times.txt')
    stops_path = os.path.join(LOCAL_GTFS_DIR, 'stops.txt')

    # Charger les trajets
    trips = pd.read_csv(trips_path, usecols=['trip_id', 'route_id'], dtype={'trip_id': 'str', 'route_id': 'str'})

    # Charger stop_times en morceaux et filtrer par stop_id
    stop_times = pd.read_csv(stop_times_path, usecols=['trip_id', 'stop_id', 'departure_time'],
                             dtype={'trip_id': 'str', 'stop_id': 'str', 'departure_time': 'str'},
                             chunksize=100000)

    relevant_trips = []
    for chunk in stop_times:
        # Extraire l'ID de base en enlevant le suffixe ":<numero_de_quai>"
        chunk['base_stop_id'] = chunk['stop_id'].str.split(':').str[0]

        # Filtrer les trajets contenant des arrêts cibles
        filtered_chunk = chunk[chunk['base_stop_id'].isin(target_stop_ids)]
        relevant_trips.append(filtered_chunk)

    relevant_stop_times = pd.concat(relevant_trips)

    # Ajouter une colonne pour le numéro de quai
    relevant_stop_times['platform_number'] = relevant_stop_times['stop_id'].str.split(':').str[-1]

    # Grouper les trajets pour vérifier la couverture complète des arrêts
    trips_with_all_stops = relevant_stop_times.groupby('trip_id')['base_stop_id'].apply(set)
    valid_trips = trips_with_all_stops[trips_with_all_stops.apply(lambda stops: target_stop_ids.issubset(stops))].index

    # Filtrer les trajets pertinents
    filtered_trips = trips[trips['trip_id'].isin(valid_trips)]

    # Enrichir avec les données des arrêts
    stops = pd.read_csv(stops_path, usecols=['stop_id', 'stop_name'], dtype={'stop_id': 'str', 'stop_name': 'str'})
    enriched_data = pd.merge(relevant_stop_times, stops, on='stop_id', how='left')
    enriched_data = pd.merge(enriched_data, filtered_trips, on='trip_id')

    # Sauvegarder les données filtrées avec la colonne du numéro de quai
    enriched_data.to_csv(filtered_trips_path, index=False)
    print(f"Fichier filtré des trajets créé : {filtered_trips_path}")

    return filtered_trips_path

def get_gtfs_rt_data(gtfs_rt_url, token=None):
    """
    Récupère les données GTFS-RT depuis un flux en ligne.
    """
    headers = {}
    if token:
        headers['Authorization'] = f'Bearer {token}'

    response = requests.get(gtfs_rt_url, headers=headers)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def calculate_next_train_with_all_stops(filtered_trips_path, gtfs_rt_feed):
    now = datetime.now(timezone.utc)
    train_stop_delays = []

    # Charger les données filtrées depuis le fichier
    filtered_data = pd.read_csv(filtered_trips_path)
    print("Nombre de trajets dans le fichier filtré :", len(filtered_data))
    print(len(gtfs_rt_feed.entity))
    # Parcourir toutes les entités dans le flux GTFS-RT
    for entity in gtfs_rt_feed.entity:
        if not entity.HasField('trip_update'):
            continue

        trip_update = entity.trip_update
        trip_id = trip_update.trip.trip_id
        print("Examining trip_id:", trip_id)

        # Vérifier si ce trip_id correspond à un train dans les données filtrées
        if trip_id in filtered_data['trip_id']:
            print("trip_id non trouvé dans le fichier filtré :", trip_id)
            continue

        # Filtrer les données pour ce train et trier par departure_time
        train_stops = filtered_data[filtered_data['trip_id'] == trip_id].copy()
        train_stops['departure_time'] = pd.to_datetime(train_stops['departure_time'], format='%H:%M:%S', errors='coerce')

        # Supprimer les lignes avec des heures de départ non valides
        train_stops.dropna(subset=['departure_time'], inplace=True)

        # Dictionnaire pour stocker les informations des arrêts
        stop_id_to_info = {row['stop_id']: row for _, row in train_stops.iterrows()}

        # Parcourir tous les arrêts pour ce train dans l'ordre de stop_sequence
        for stop_time_update in trip_update.stop_time_update:
            stop_id = stop_time_update.stop_id
            print("Examining stop_id:", stop_id)

            # Récupérer les informations pour cet arrêt
            if stop_id not in stop_id_to_info:
                print("stop_id non trouvé dans les données filtrées :", stop_id)
                continue

            stop_info = stop_id_to_info[stop_id]
            scheduled_time = stop_info['departure_time']
            delay = stop_time_update.departure.delay if stop_time_update.HasField('departure') else 0

            # Convertir scheduled_time en datetime en UTC
            scheduled_time = scheduled_time.tz_localize('UTC')

            # Vérifier si le train est à venir
            if scheduled_time > now:
                train_stop_delays.append({
                    'stop_name': stop_info['stop_name'],
                    'scheduled_time': scheduled_time.strftime('%H:%M:%S'),
                    'actual_delay': delay,
                    'delay_in_minutes': delay / 60
                })

        # Fin de la boucle sur les arrêts
        if train_stop_delays:
            print(f"Détails du prochain train à l'arrêt {stop_info['stop_name']}: ", train_stop_delays[-1])

    # Sortie des résultats triés par heure de départ
    if train_stop_delays:
        result_df = pd.DataFrame(train_stop_delays)
        result_df = result_df.sort_values(by='scheduled_time')  # Trier par heure de départ
        print("Détails du prochain train avec tous les arrêts :", result_df)
        return result_df

    print("Aucun train trouvé pour les conditions actuelles.")
    return pd.DataFrame()  # Retourner un DataFrame vide si aucun train trouvé



# Exécution principale
if __name__ == "__main__":
    # Assurez-vous que les données GTFS statiques sont à jour
    ensure_local_gtfs()

    trips = save_trips_related_to_stops()

    # Charger les données GTFS-RT
    gtfs_rt_feed = get_gtfs_rt_data(GTFS_RT_URL, token=TOKEN)

    # Calculer le retard du prochain train
    next_train_delay_df = calculate_next_train_with_all_stops(trips, gtfs_rt_feed)

    # Afficher le retard du prochain train
    if not next_train_delay_df.empty:
        print("Retard du prochain train :")
        print(next_train_delay_df)
    else:
        print("Aucun train à venir trouvé.")
