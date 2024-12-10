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
LOCAL_GTFS_ZIP = os.path.join(LOCAL_GTFS_DIR, "gtfs_static.zip")


def ensure_local_gtfs():
    """
    Vérifie si le fichier GTFS statique local est à jour, sinon le télécharge.
    """
    os.makedirs(LOCAL_GTFS_DIR, exist_ok=True)

    # Obtenir le nom du fichier distant
    response = requests.head(GTFS_STATIC_URL)
    response.raise_for_status()
    remote_zip_name = os.path.basename(response.headers.get('Location', 'gtfs_static.zip'))
    remote_zip_path = os.path.join(LOCAL_GTFS_DIR, remote_zip_name)

    # Si le fichier local existe déjà, ne rien faire
    if os.path.exists(remote_zip_path):
        print("Le fichier GTFS statique est déjà à jour.")
        return remote_zip_path

    # Sinon, télécharger et extraire
    print("Téléchargement des données GTFS statiques...")
    response = requests.get(GTFS_STATIC_URL)
    response.raise_for_status()
    with open(remote_zip_path, 'wb') as f:
        f.write(response.content)

    # Extraire les fichiers
    with zipfile.ZipFile(remote_zip_path, 'r') as z:
        z.extractall(LOCAL_GTFS_DIR)

    # Nettoyer les anciens fichiers
    for file in os.listdir(LOCAL_GTFS_DIR):
        if file != remote_zip_name:
            os.remove(os.path.join(LOCAL_GTFS_DIR, file))

    print("Mise à jour des données GTFS statiques terminée.")
    return remote_zip_path


def get_line_ids(line_name, cff_id):
    """
    Trouve tous les IDs de lignes (route_id) correspondant à un nom de ligne et un CFF ID.
    """
    routes_path = os.path.join(LOCAL_GTFS_DIR, 'routes.txt')
    routes = pd.read_csv(routes_path, usecols=['route_id', 'route_short_name', 'agency_id'], dtype='str')

    # Filtrer par nom de ligne et ID de l'agence (CFF ID)
    matched_routes = routes[
        (routes['route_short_name'] == line_name) &
        (routes['agency_id'] == cff_id)
    ]

    if matched_routes.empty:
        raise ValueError(f"Aucune ligne trouvée pour le nom : {line_name} et CFF ID : {cff_id}")

    return matched_routes['route_id'].tolist()


def save_filtered_data(line_id):
    """
    Filtre les données GTFS statiques pour une ligne et les sauvegarde localement.
    """
    filtered_data_path = os.path.join(LOCAL_GTFS_DIR, f"filtered_data_line_{line_id}.csv")

    if os.path.exists(filtered_data_path):
        print(f"Fichier filtré déjà existant pour la ligne {line_id}: {filtered_data_path}")
        return filtered_data_path

    trips_path = os.path.join(LOCAL_GTFS_DIR, 'trips.txt')
    stop_times_path = os.path.join(LOCAL_GTFS_DIR, 'stop_times.txt')
    stops_path = os.path.join(LOCAL_GTFS_DIR, 'stops.txt')

    # Charger les trajets (trips.txt) et filtrer par route_id
    trips = pd.read_csv(trips_path, usecols=['trip_id', 'route_id'], dtype={'trip_id': 'str', 'route_id': 'str'})
    line_trips = trips[trips['route_id'] == line_id]

    trip_ids = set(line_trips['trip_id'])

    # Charger stop_times en morceaux et filtrer par trip_id
    stop_times = pd.read_csv(stop_times_path, usecols=['trip_id', 'stop_id', 'departure_time'],
                             dtype={'trip_id': 'str', 'stop_id': 'str', 'departure_time': 'str'},
                             chunksize=100000)

    filtered_stop_times = pd.concat(
        chunk[chunk['trip_id'].isin(trip_ids)] for chunk in stop_times
    )

    # Charger les données des arrêts
    stops = pd.read_csv(stops_path, usecols=['stop_id', 'stop_name'], dtype={'stop_id': 'str', 'stop_name': 'str'})

    # Merge pour enrichir les données finales
    filtered_data = pd.merge(filtered_stop_times, line_trips, on='trip_id')
    filtered_data = pd.merge(filtered_data, stops, on='stop_id')

    filtered_data.to_csv(filtered_data_path, index=False)
    print(f"Fichier filtré créé pour la ligne {line_id}: {filtered_data_path}")

    return filtered_data_path


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


def calculate_next_train_delay(static_data, gtfs_rt_feed):
    """
    Calcule le retard pour le prochain train d'une ligne spécifique.
    """
    now = datetime.now(timezone.utc)
    next_train_delay = []

    for line_id, data_path in static_data.items():
        if not data_path:
            continue  # Ligne ignorée

        filtered_data = pd.read_csv(data_path)

        # Chercher le prochain départ
        for entity in gtfs_rt_feed.entity:
            if not entity.HasField('trip_update'):
                continue

            trip_update = entity.trip_update
            trip_id = trip_update.trip.trip_id

            if trip_id not in filtered_data['trip_id'].values:
                continue

            for stop_time_update in trip_update.stop_time_update:
                stop_id = stop_time_update.stop_id
                if stop_time_update.HasField('departure'):
                    delay = stop_time_update.departure.delay

                    stop_name = filtered_data.loc[filtered_data['stop_id'] == stop_id, 'stop_name'].values[0]
                    scheduled_time = filtered_data.loc[
                        (filtered_data['trip_id'] == trip_id) & (filtered_data['stop_id'] == stop_id),
                        'departure_time'
                    ].values[0]

                    # Convertir le temps en secondes (heure:min:sec)
                    h, m, s = map(int, scheduled_time.split(":"))
                    scheduled_secs = h * 3600 + m * 60 + s

                    # Comparer avec l'heure actuelle pour trouver le prochain train
                    scheduled_datetime = datetime.fromtimestamp(scheduled_secs, timezone.utc)

                    if scheduled_datetime > now:
                        next_train_delay.append({
                            'stop_name': stop_name,
                            'scheduled_time': scheduled_datetime.strftime('%H:%M:%S'),
                            'actual_delay': delay,
                            'delay_in_minutes': delay / 60
                        })
                        # Sortir après avoir trouvé le prochain train
                        break
            if next_train_delay:
                break  # Sortir de la boucle externe si le prochain train a été trouvé

    return pd.DataFrame(next_train_delay)


# Exécution principale
if __name__ == "__main__":
    # Assurez-vous que les données GTFS statiques sont à jour
    ensure_local_gtfs()

    # Récupérer les IDs de lignes à partir du nom de ligne et de l'ID de l'agence
    line_ids = get_line_ids(LINE_NAME, CFF_ID)

    # Sauvegarder les données filtrées pour chaque ligne
    static_data = {}
    for line_id in line_ids:
        static_data[line_id] = save_filtered_data(line_id)

    # Charger les données GTFS-RT
    gtfs_rt_feed = get_gtfs_rt_data(GTFS_RT_URL, token=TOKEN)

    # Calculer le retard du prochain train
    next_train_delay_df = calculate_next_train_delay(static_data, gtfs_rt_feed)

    # Afficher le retard du prochain train
    if not next_train_delay_df.empty:
        print("Retard du prochain train :")
        print(next_train_delay_df)
    else:
        print("Aucun train à venir trouvé.")
