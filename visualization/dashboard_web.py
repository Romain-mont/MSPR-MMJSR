import os
import pandas as pd
import streamlit as st
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from dotenv import load_dotenv

load_dotenv()
API_URL = os.getenv("API_URL", "http://localhost:8000")

sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)
plt.rcParams["font.size"] = 10

NUIT_TYPES = {"EuroNight", "Nightjet", "Train Nuit", "Intercités Nuit",
              "Train Longue Distance Nuit", "TGV Nuit", "ICE Nuit",
              "InterCity Nuit", "EuroCity Nuit"}

# ---------------------------------------------------------------------------
# Chargement données
# ---------------------------------------------------------------------------
@st.cache_data(ttl=600, show_spinner=False)
def load_data() -> pd.DataFrame:
    try:
        r = requests.get(f"{API_URL}/data", timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        st.error(f"Erreur de connexion à l'API : {e}")
        return pd.DataFrame()

    df["distance_km"] = pd.to_numeric(df["distance_km"], errors="coerce")
    df["co2_kg"]      = pd.to_numeric(df["co2_kg"],      errors="coerce")
    df["facteur_co2"] = pd.to_numeric(df["facteur_co2"], errors="coerce")
    df = df.dropna(subset=["distance_km", "co2_kg", "vehicule_type", "origine", "destination"])
    df = df[(df["distance_km"] > 0) & (df["co2_kg"] >= 0)]

    # Classification
    df["mode"]       = df["vehicule_type"].apply(lambda v: "Avion" if "Avion" in str(v) else "Train")
    df["train_type"] = df["vehicule_type"].apply(
        lambda v: "Nuit" if (str(v) in NUIT_TYPES or "Nuit" in str(v)) else
                  "Jour" if "Avion" not in str(v) else None
    )
    df["route"]      = df["origine"] + " → " + df["destination"]
    # Clé métier de comparaison: ville -> ville (train et avion comparables)
    if "origine_ville" in df.columns and "destination_ville" in df.columns:
        df["city_route"] = df["origine_ville"] + " → " + df["destination_ville"]
    else:
        df["city_route"] = df["route"]
    df["co2_par_km"] = df["co2_kg"] / df["distance_km"]

    return df


# ---------------------------------------------------------------------------
# Helpers graphiques
# ---------------------------------------------------------------------------
def bar_co2_by_mode(df):
    agg = df.groupby("mode", as_index=False)["co2_kg"].mean().sort_values("co2_kg", ascending=False)
    fig, ax = plt.subplots(figsize=(7, 4))
    colors = ["#e74c3c" if m == "Avion" else "#2ecc71" for m in agg["mode"]]
    ax.bar(agg["mode"], agg["co2_kg"], color=colors, edgecolor="white", width=0.5)
    for i, (_, row) in enumerate(agg.iterrows()):
        ax.text(i, row["co2_kg"] + 0.5, f"{row['co2_kg']:.1f} kg", ha="center", fontweight="bold")
    ax.set_ylabel("CO₂ moyen par trajet (kg)")
    ax.set_title("CO₂ moyen : Train vs Avion")
    plt.tight_layout()
    return fig

def scatter_train_vs_avion(df):
    fig, ax = plt.subplots(figsize=(10, 5))
    for mode, color in [("Train", "#2ecc71"), ("Avion", "#e74c3c")]:
        sub = df[df["mode"] == mode]
        ax.scatter(sub["distance_km"], sub["co2_kg"], alpha=0.4, s=15,
                   label=mode, color=color)
    ax.set_xlabel("Distance (km)")
    ax.set_ylabel("CO₂ (kg)")
    ax.set_title("CO₂ en fonction de la distance — Train vs Avion")
    ax.legend()
    plt.tight_layout()
    return fig

def bar_co2_intensity(df):
    agg = df.groupby("vehicule_type", as_index=False)["co2_par_km"].mean()
    agg = agg.sort_values("co2_par_km", ascending=True)
    fig, ax = plt.subplots(figsize=(10, max(4, len(agg) * 0.4)))
    colors = ["#e74c3c" if "Avion" in v
              else "#3498db" if (v in NUIT_TYPES or "Nuit" in v)
              else "#2ecc71"
              for v in agg["vehicule_type"]]
    ax.barh(agg["vehicule_type"], agg["co2_par_km"], color=colors, edgecolor="white")
    ax.set_xlabel("kg CO₂ / km")
    ax.set_title("Intensité carbone par type de véhicule (kg CO₂/km)")
    plt.tight_layout()
    return fig

def bar_top_economies(df, n=15):
    trains = df[df["mode"] == "Train"].groupby("city_route", as_index=False)["co2_kg"].mean().rename(columns={"co2_kg": "co2_train"})
    avions = df[df["mode"] == "Avion"].groupby("city_route", as_index=False)["co2_kg"].mean().rename(columns={"co2_kg": "co2_avion"})
    merged = trains.merge(avions, on="city_route")
    merged = merged.rename(columns={"city_route": "route"})
    merged["economie_kg"] = merged["co2_avion"] - merged["co2_train"]
    merged["reduction_pct"] = (merged["economie_kg"] / merged["co2_avion"] * 100).round(1)
    merged = merged[merged["economie_kg"] > 0].nlargest(n, "economie_kg")

    fig, ax = plt.subplots(figsize=(12, max(5, len(merged) * 0.5)))
    bars = ax.barh(merged["route"], merged["economie_kg"], color="#27ae60", edgecolor="white")
    for bar, pct in zip(bars, merged["reduction_pct"]):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                f"−{pct:.0f}%", va="center", fontsize=8, color="#27ae60")
    ax.invert_yaxis()
    ax.set_xlabel("kg CO₂ économisés par trajet (vs avion)")
    ax.set_title(f"Top {n} routes — économie CO₂ en choisissant le train")
    plt.tight_layout()
    return fig, merged

def pie_jour_nuit(df):
    sub = df[df["mode"] == "Train"]
    counts = sub["train_type"].value_counts()
    fig, ax = plt.subplots(figsize=(5, 5))
    colors = ["#3498db", "#9b59b6"]
    ax.pie(counts, labels=counts.index, autopct="%1.1f%%", colors=colors,
           startangle=90, wedgeprops={"edgecolor": "white"})
    ax.set_title("Répartition Trains Jour / Nuit")
    plt.tight_layout()
    return fig

def bar_jour_nuit_co2(df):
    sub = df[df["mode"] == "Train"].dropna(subset=["train_type"])
    agg = sub.groupby("train_type", as_index=False).agg(
        co2_moyen=("co2_kg", "mean"),
        nb_trajets=("co2_kg", "count"),
    )
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    colors = ["#3498db", "#9b59b6"]
    axes[0].bar(agg["train_type"], agg["co2_moyen"], color=colors, edgecolor="white", width=0.4)
    axes[0].set_title("CO₂ moyen par trajet")
    axes[0].set_ylabel("CO₂ (kg)")
    axes[1].bar(agg["train_type"], agg["nb_trajets"], color=colors, edgecolor="white", width=0.4)
    axes[1].set_title("Nombre de trajets")
    axes[1].set_ylabel("Trajets")
    plt.tight_layout()
    return fig

def quality_report(df):
    rep = {}
    rep["rows"]         = len(df)
    rep["nulls_total"]  = int(df.isna().sum().sum())
    rep["nulls_by_col"] = df.isna().sum().sort_values(ascending=False)
    key_cols = ["origine", "destination", "vehicule_type", "distance_km", "co2_kg"]
    rep["duplicates"]   = int(df.duplicated(subset=key_cols).sum())
    q1 = df["co2_kg"].quantile(0.25)
    q3 = df["co2_kg"].quantile(0.75)
    iqr = q3 - q1
    low, high = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    rep["outliers_co2"]    = int(((df["co2_kg"] < low) | (df["co2_kg"] > high)).sum())
    rep["co2_iqr_bounds"]  = (float(low), float(high))
    return rep


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
st.set_page_config(page_title="ObRail — Comparateur CO₂ Ferroviaire", layout="wide")
st.title("🚆 ObRail Europe — Comparateur CO₂ : Train vs Avion")
st.caption("Source : Mobility Database (GTFS) · Back on Track · OurAirports · Référentiel ADEME")

if st.button("🔄 Actualiser les données"):
    st.cache_data.clear()
    st.rerun()

with st.spinner("Chargement des données…"):
    df = load_data()

if df.empty:
    st.warning("Aucune donnée disponible. Vérifiez que l'API et la base de données sont démarrées.")
    st.stop()

# Sidebar filtre distance
st.sidebar.header("Filtres")
distance_min, distance_max = int(df["distance_km"].min()), int(df["distance_km"].max())
dist_range = st.sidebar.slider("Distance (km)", distance_min, distance_max,
                                (distance_min, distance_max), step=50)
df = df[(df["distance_km"] >= dist_range[0]) & (df["distance_km"] <= dist_range[1])]

# KPIs globaux
nb_train      = len(df[df["mode"] == "Train"])
nb_avion      = len(df[df["mode"] == "Avion"])
co2_moy_train = df[df["mode"] == "Train"]["co2_kg"].mean()
co2_moy_avion = df[df["mode"] == "Avion"]["co2_kg"].mean()
ratio = co2_moy_avion / co2_moy_train if co2_moy_train > 0 else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Trajets ferroviaires",      f"{nb_train:,}".replace(",", " "))
k2.metric("Trajets aériens (virtuels)", f"{nb_avion:,}".replace(",", " "))
k3.metric("CO₂ moyen Train",           f"{co2_moy_train:.1f} kg")
k4.metric("CO₂ moyen Avion",           f"{co2_moy_avion:.1f} kg")
k5.metric("L'avion émet en moyenne",   f"× {ratio:.1f} plus de CO₂")

st.divider()

tabs = st.tabs([
    "✈️🚆 Train vs Avion",
    "🌙 Trains Jour vs Nuit",
    "🏆 Top économies CO₂",
    "🧪 Qualité des données",
])

# TAB 1 — Train vs Avion
with tabs[0]:
    st.subheader("Comparaison de l'empreinte carbone : Train vs Avion")

    c1, c2 = st.columns(2)
    with c1:
        st.pyplot(bar_co2_by_mode(df))
    with c2:
        st.pyplot(scatter_train_vs_avion(df))

    st.subheader("Intensité carbone par type de véhicule (kg CO₂ / km)")
    st.pyplot(bar_co2_intensity(df))
    st.caption("🟢 Train jour  🔵 Train nuit  🔴 Avion — Référentiel ADEME")

# TAB 2 — Jour vs Nuit
with tabs[1]:
    st.subheader("Trains de Jour vs Trains de Nuit")

    trains_df = df[df["mode"] == "Train"]
    if trains_df.empty:
        st.info("Pas de données ferroviaires disponibles.")
    else:
        c1, c2 = st.columns([1, 2])
        with c1:
            st.pyplot(pie_jour_nuit(trains_df))
        with c2:
            st.pyplot(bar_jour_nuit_co2(trains_df))

        st.subheader("Détail par type de train")
        agg_type = (
            trains_df.groupby("vehicule_type", as_index=False)
            .agg(nb_trajets=("co2_kg", "count"),
                 co2_moyen=("co2_kg", "mean"),
                 distance_moyenne=("distance_km", "mean"))
            .sort_values("co2_moyen")
        )
        agg_type["co2_moyen"]        = agg_type["co2_moyen"].round(2)
        agg_type["distance_moyenne"] = agg_type["distance_moyenne"].round(1)
        st.dataframe(agg_type, use_container_width=True)

# TAB 3 — Top économies
with tabs[2]:
    st.subheader("Routes où prendre le train est le plus bénéfique pour le climat")
    n = st.slider("Nombre de routes à afficher", 5, 30, 15, 5)
    fig, table = bar_top_economies(df, n=n)

    if table.empty:
        st.info("Pas assez de routes communes entre train et avion pour comparer. "
                "Vérifiez que les données intermodales sont bien chargées.")
    else:
        st.pyplot(fig)
        table_display = table[["route", "co2_train", "co2_avion", "economie_kg", "reduction_pct"]].copy()
        table_display.columns = ["Route", "CO₂ Train (kg)", "CO₂ Avion (kg)", "Économie (kg)", "Réduction (%)"]
        table_display = table_display.round(2)
        st.dataframe(table_display, use_container_width=True)

        st.success(
            f"Sur ces {len(table)} routes, choisir le train plutôt que l'avion économise en moyenne "
            f"**{table['economie_kg'].mean():.0f} kg de CO₂** par trajet "
            f"(−{table['reduction_pct'].mean():.0f}%)."
        )

# TAB 4 — Qualité
with tabs[3]:
    st.subheader("Contrôle qualité des données")
    rep = quality_report(df)

    q1, q2, q3, q4 = st.columns(4)
    q1.metric("Lignes totales",        f"{rep['rows']:,}".replace(",", " "))
    q2.metric("Valeurs nulles",        rep["nulls_total"])
    q3.metric("Doublons (clé logique)", rep["duplicates"])
    q4.metric("Outliers CO₂ (IQR)",    rep["outliers_co2"])
    st.caption(f"Bornes IQR CO₂ : {rep['co2_iqr_bounds'][0]:.2f} → {rep['co2_iqr_bounds'][1]:.2f} kg")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Répartition par mode")
        mode_counts = df["mode"].value_counts().reset_index()
        mode_counts.columns = ["Mode", "Trajets"]
        st.dataframe(mode_counts, use_container_width=True)

    with c2:
        st.subheader("Nulls par colonne")
        st.dataframe(rep["nulls_by_col"].rename("nulls").to_frame(), use_container_width=True)

    st.subheader("Échantillon de données brutes")
    st.dataframe(df.sample(min(200, len(df)), random_state=42), use_container_width=True, height=300)
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Télécharger CSV", data=csv, file_name="obrail_co2_data.csv", mime="text/csv")
