import sys
import argparse
from datetime import datetime

from .ingestion.minio_client import read_csv_from_minio
from .ingestion.transformer import transform_and_split
from .ingestion.postgres_loader import load_to_postgres
from .utils.logger import setup_logger

logger = setup_logger("main")


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Format invalide: '{date_str}'. Attendu: YYYYMMDD"
        )


def main():
    parser = argparse.ArgumentParser(description="Ingestion des ventes fashion store")
    parser.add_argument("date", type=parse_date, help="Date de vente à ingérer (YYYYMMDD)")
    args = parser.parse_args()

    target_date = args.date.date()
    logger.info(f"Ingestion démarrée pour {target_date}")

    try:
        df = read_csv_from_minio()
        logger.info(f"{len(df)} lignes lues depuis Minio")

        tables = transform_and_split(df, target_date)
        if tables is None:
            logger.warning(f"Aucune donnée pour {target_date}")
            sys.exit(0)

        logger.info(f"{len(tables['sale_items'])} articles à charger")
        load_to_postgres(tables)
        logger.info("Ingestion terminee avec succes")

    except Exception as e:
        logger.error(f"Echec de l'ingestion: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
