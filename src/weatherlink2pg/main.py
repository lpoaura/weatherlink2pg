"""Application click permettant la récupération des données de la sonde
météorologique soit depuis le début soit depuis la dernière TS enregistrée.
Et ce jusqu'à 00h00 du jour J"""

# 1 : Librairies et options
import warnings
from importlib.metadata import metadata
from typing import Optional

import click

from .helpers import (
    echo_default,
    echo_failure,
    echo_info,
    echo_success,
    get_station_ids,
    get_stations_infos,
    last_ts_bdd,
    one_day_data,
    start_station,
    today_ts,
    up_to_bdd,
)

# Ignorer les avertissements FutureWarning : colonnes 100% NaN
warnings.filterwarnings("ignore", category=FutureWarning)


# 4 : Utilisation de la routine de récupération des données via click :
@click.group()
# @click.option("--debug", default=False)
def cli():
    """Weatherlink2PG CLI app"""
    # echo_success(f"Debug mode is {'on' if debug else 'off'}")


@cli.command("version")
def get_version():
    """Get version"""

    app_metadata = metadata("weatherlink2pg")
    echo_default(app_metadata["name"])
    echo_info(f"version {app_metadata['Version']}")
    echo_info(app_metadata["Summary"])


@cli.command("stations")
def get_stations():
    """Get version"""
    get_stations_infos()


@cli.command("full")
@click.option(
    "--since",
    "-s",
    help="Date la plus ancienne à laquelle remonter (format AAAA-MM-JJ)",
)
@click.option("--station_id", "-i", help="ID de la station")
def full(since: Optional[str] = None, station_id: Optional[str] = None):
    """Commande de récupération des donnes depuis le début de la sonde
    avec une réinitialisation de la table."""
    echo_info("Lancement du script de téléchargement complet des données")

    stations = (
        get_station_ids()
        if not station_id
        else [
            station_id,
        ]
    )
    end_api = today_ts()
    first_day_station, if_exists_bdd = start_station(since)
    df_news = one_day_data(stations, first_day_station, end_api)
    up_to_bdd(df_news, if_exists_bdd)
    echo_success("Le script s'est exécuté avec succès.")


@cli.command("update")
@click.option("--station_id", "-i", help="ID de la station")
def update(station_id: Optional[str] = None):
    """Commande de mise à jour et d'ajout des données à la table."""

    echo_info("Lancement du script de mise à jour des données")
    stations = (
        get_station_ids()
        if not station_id
        else [
            station_id,
        ]
    )
    end_api = today_ts()
    for station in stations:
        last_ts, if_exists_bdd = last_ts_bdd(station)
        df_news = one_day_data(station, last_ts or start_station, end_api)
        up_to_bdd(df_news, if_exists_bdd)
    echo_success("Le script s'est exécuté avec succès.")


if __name__ == "__main__":
    try:
        cli()
    except click.UsageError as e:
        echo_failure(f"Erreur d'utilisation : {e}")
    except click.Abort:
        echo_failure("L'opération a été interrompue.")
