import pandas as pd

sheet_id = "15zsK-lBuibUtZ1s2FxVHvAmSu-pEuE0NDT6CAMYL2TY"

# Liste des onglets importants
onglets = ["agencies", "routes", "trips", "stops", "calendar", "trip_stop"]

for onglet in onglets:
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={onglet}"
    try:
        df = pd.read_csv(url)
        df.to_csv(f"back_on_track_{onglet}.csv", index=False)
        print(f"✓ {onglet}.csv téléchargé ({len(df)} lignes)")
    except Exception as e:
        print(f"✗ Erreur sur {onglet}: {e}")
