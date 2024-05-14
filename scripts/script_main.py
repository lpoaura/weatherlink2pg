"""Application click permettant la récupération des données de la sonde
météorologique soit depuis le début soit depuis la dernière TS enregistrée.
Et ce jusqu'à 00h00 du jour J"""

# 1 : Librairies et options
import warnings

import click
from script_def import (
    echo_failure,
    echo_success,
    last_ts_bdd,
    one_day_data,
    start_station,
    today_ts,
    up_to_bdd,
)

# Ignorer les avertissements FutureWarning : colonnes 100% NaN
warnings.filterwarnings("ignore", category=FutureWarning)


# 2 : Utilisation de la routine de récupération des données via click :
@click.command()
@click.option(
    "--full",
    is_flag=True,
    help="""Option de récupération des donnes depuis le début de la sonde
    avec une réinitialisation de la table.""",
)
@click.option(
    "--update",
    is_flag=True,
    help="Option de mise à jour et d'ajout des données à la table.",
)
def main(full, update):
    """Permet à la fonction click de choisir entre récupérer toutes les données
    ou updater les nouvelles données."""

    if not (full or update):
        raise click.UsageError(
            """Vous devez spécifier une des options:
                               --full ou --update."""
        )

    if full:
        echo_success(
            """Le script de récupération totale des données
                     a été lancé avec succés."""
        )
        first_day_station, if_exists_bdd = start_station()
        end_api = today_ts()
        df_news = one_day_data(first_day_station, end_api)
        up_to_bdd(df_news, if_exists_bdd)
        echo_success("Le script s'est exécuté avec succès.")

    if update:
        echo_success(
            """Le script de mise à jour des données
                     a été lancé avec succès."""
        )
        last_ts, if_exists_bdd = last_ts_bdd()
        end_api = today_ts()
        df_news = one_day_data(last_ts, end_api)
        up_to_bdd(df_news, if_exists_bdd)
        echo_success("Le script s'est exécuté avec succès.")


if __name__ == "__main__":
    try:
        main()
    except click.UsageError as e:
        echo_failure(f"Erreur d'utilisation : {e}")
    except click.Abort:
        echo_failure("L'opération a été interrompue.")
