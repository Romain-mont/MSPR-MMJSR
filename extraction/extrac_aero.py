import argparse
import datetime as dt
import sys
from pathlib import Path
from urllib.request import urlopen, Request

DATA_URL = "https://ourairports.com/data/airports.csv"


def download_airports(output_dir: Path, overwrite: bool) -> Path:
    # Crée le dossier de sortie si besoin
    output_dir.mkdir(parents=True, exist_ok=True)

    # Utilise la date du jour dans le nom du fichier (un fichier par jour)
    today = dt.date.today().strftime("%Y-%m-%d")
    filename = f"airports_{today}.csv"
    output_path = output_dir / filename

    # Si le fichier du jour existe déjà et qu'on ne veut pas le réécrire
    if output_path.exists() and not overwrite:
        return output_path

    # Télécharge le fichier depuis OurAirports
    req = Request(DATA_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=60) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP error {response.status}")
        content = response.read()

    # Écrit le contenu sur disque
    output_path.write_bytes(content)
    return output_path


def main() -> int:
    # Lit les options en ligne de commande
    parser = argparse.ArgumentParser(description="Download daily airports.csv from OurAirports.")
    parser.add_argument(
        "--output",
        "-o",
        default="data",
        help="Output folder for downloaded files (default: data)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite today's file if it already exists",
    )
    args = parser.parse_args()

    output_dir = Path(args.output)
    try:
        # Lance le téléchargement
        output_path = download_airports(output_dir, args.overwrite)
    except Exception as exc:
        # En cas d'erreur, on affiche un message clair
        print(f"Download failed: {exc}", file=sys.stderr)
        return 1

    # Message de succès
    print(f"Saved: {output_path}")
    return 0


if __name__ == "__main__":
    # Point d'entrée du script
    raise SystemExit(main())
