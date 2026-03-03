import os
import time
import pandas as pd
import streamlit as st
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from dotenv import load_dotenv

# ----------------------------
# Config / Style
# ----------------------------
load_dotenv()

# Configuration de l'API
API_URL = os.getenv("API_URL", "http://localhost:8000")

sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)
plt.rcParams["font.size"] = 10

# ----------------------------
# Data
# ----------------------------
@st.cache_data(ttl=600, show_spinner=False)
def load_data() -> pd.DataFrame:
    """Charge les données via l'API REST."""
    try:
        response = requests.get(f"{API_URL}/data", timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Erreur de connexion à l'API: {e}")
        st.info(f"URL utilisée: {API_URL}/data")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Erreur lors du chargement des données: {e}")
        return pd.DataFrame()

    # Nettoyage léger
    df["distance_km"] = pd.to_numeric(df["distance_km"], errors="coerce")
    df["co2_kg"] = pd.to_numeric(df["co2_kg"], errors="coerce")
    df["facteur_co2"] = pd.to_numeric(df["facteur_co2"], errors="coerce")

    df = df.dropna(subset=["distance_km", "co2_kg", "vehicule_type", "origine", "destination"])
    df = df[df["distance_km"] >= 0]
    df = df[df["co2_kg"] >= 0]

    df["route"] = df["origine"].astype(str) + " → " + df["destination"].astype(str)
    df["categorie_distance"] = df["is_long_distance"].map(
        {True: "Longue (>500km)", False: "Courte (≤500km)"}
    )
    df["co2_par_km"] = df["co2_kg"] / df["distance_km"].replace(0, pd.NA)
    df["co2_par_km"] = df["co2_par_km"].fillna(0)

    return df

def sample_df(df: pd.DataFrame, max_rows: int) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df
    return df.sample(n=max_rows, random_state=42)

# ----------------------------
# Plots
# ----------------------------
def fig_hist_with_mean(series, title, xlabel):
    fig, ax = plt.subplots()
    sns.histplot(series, bins=50, kde=True, ax=ax)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Nombre")
    ax.axvline(series.mean(), linestyle="--", label=f"Moyenne: {series.mean():.2f}")
    ax.legend()
    plt.tight_layout()
    return fig

def fig_cdf(series, title, xlabel):
    s = series.sort_values().reset_index(drop=True)
    y = (s.index + 1) / len(s)
    fig, ax = plt.subplots()
    ax.plot(s, y)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Proportion cumulée")
    plt.tight_layout()
    return fig

def fig_scatter(df):
    fig, ax = plt.subplots()
    sns.scatterplot(
        data=df, x="distance_km", y="co2_kg", hue="vehicule_type",
        alpha=0.55, s=18, ax=ax
    )
    ax.set_title("CO2 en fonction de la distance")
    ax.set_xlabel("Distance (km)")
    ax.set_ylabel("CO2 (kg)")
    plt.tight_layout()
    return fig

def fig_box_vehicle(df, ycol, title, ylabel):
    fig, ax = plt.subplots(figsize=(12, 5))
    sns.boxplot(data=df, x="vehicule_type", y=ycol, ax=ax)
    ax.set_title(title)
    ax.set_xlabel("")
    ax.set_ylabel(ylabel)
    plt.tight_layout()
    return fig

def fig_violin_vehicle(df, ycol, title, ylabel):
    fig, ax = plt.subplots(figsize=(12, 5))
    sns.violinplot(data=df, x="vehicule_type", y=ycol, inner="quartile", ax=ax)
    ax.set_title(title)
    ax.set_xlabel("")
    ax.set_ylabel(ylabel)
    plt.tight_layout()
    return fig

def fig_heatmap_corr(df):
    num = df[["distance_km", "co2_kg", "facteur_co2", "co2_par_km"]].copy()
    corr = num.corr(numeric_only=True)
    fig, ax = plt.subplots(figsize=(7, 5))
    sns.heatmap(corr, annot=True, fmt=".2f", ax=ax)
    ax.set_title("Corrélations (numériques)")
    plt.tight_layout()
    return fig

def fig_top_routes(df, n=15, metric="co2_sum"):
    agg = df.groupby("route", as_index=False).agg(
        distance_mean=("distance_km", "mean"),
        co2_mean=("co2_kg", "mean"),
        co2_sum=("co2_kg", "sum"),
        trips=("route", "count"),
    )
    top = agg.nlargest(n, metric)

    fig, ax = plt.subplots(figsize=(12, 8))
    ax.barh(top["route"], top[metric])
    ax.invert_yaxis()
    ax.set_title(f"Top {n} routes par {metric}")
    ax.set_xlabel(metric)
    plt.tight_layout()
    return fig, top

def fig_co2_by_category(df):
    agg = df.groupby("categorie_distance", as_index=False).agg(
        co2_sum=("co2_kg", "sum"),
        co2_mean=("co2_kg", "mean"),
        trips=("co2_kg", "count"),
        dist_sum=("distance_km", "sum"),
    )
    fig, ax = plt.subplots(figsize=(10, 4))
    sns.barplot(data=agg, x="categorie_distance", y="co2_sum", ax=ax)
    ax.set_title("CO2 total par catégorie (courte/longue)")
    ax.set_xlabel("")
    ax.set_ylabel("CO2 total (kg)")
    plt.tight_layout()
    return fig, agg

def quality_report(df):
    rep = {}
    rep["rows"] = len(df)
    rep["nulls_total"] = int(df.isna().sum().sum())
    rep["nulls_by_col"] = df.isna().sum().sort_values(ascending=False)

    # Doublons “logiques”
    key_cols = ["origine", "destination", "vehicule_type", "distance_km", "co2_kg"]
    rep["duplicates"] = int(df.duplicated(subset=key_cols).sum())

    # Anomalies simples
    rep["negative_distance"] = int((df["distance_km"] < 0).sum())
    rep["negative_co2"] = int((df["co2_kg"] < 0).sum())

    # Outliers IQR sur co2_kg
    q1 = df["co2_kg"].quantile(0.25)
    q3 = df["co2_kg"].quantile(0.75)
    iqr = q3 - q1
    low = q1 - 1.5 * iqr
    high = q3 + 1.5 * iqr
    rep["outliers_co2_iqr"] = int(((df["co2_kg"] < low) | (df["co2_kg"] > high)).sum())
    rep["co2_iqr_bounds"] = (float(low), float(high))

    return rep

# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="Datamart CO2 — Dashboard", layout="wide")
st.title("🌍 Datamart CO2 — Dashboard (v2)")
st.caption("Moins de filtres, plus de vision. Et un gros bouton refresh parce qu’on aime souffrir proprement.")

# Actualiseur
cA, cB, cC = st.columns([1, 2, 2])
with cA:
    if st.button("🔄 Actualiser les données"):
        st.cache_data.clear()
        st.rerun()

with cB:
    auto = st.toggle("⏱️ Auto-refresh", value=False)
with cC:
    interval = st.number_input("Intervalle (secondes)", min_value=5, max_value=600, value=60, step=5)
    if auto:
        # rerun soft
        time.sleep(int(interval))
        st.rerun()

# Load
with st.spinner("Chargement depuis PostgreSQL…"):
    try:
        df = load_data()
    except Exception as e:
        st.error("Erreur de connexion à la base de données.")
        st.code(str(e))
        st.stop()

if df.empty:
    st.warning("Aucune donnée trouvée.")
    st.stop()

# Filtres (light)
st.sidebar.header("🎛️ Filtres (light)")
vehicules = sorted(df["vehicule_type"].unique().tolist())
vsel = st.sidebar.multiselect("Véhicules", vehicules, default=vehicules)

cat = st.sidebar.radio("Catégorie distance", ["Toutes", "Courte (≤500km)", "Longue (>500km)"], index=0)
max_rows = st.sidebar.slider("Échantillon max (perf)", 2000, 50000, 20000, 2000)

filtered = df[df["vehicule_type"].isin(vsel)].copy()
if cat != "Toutes":
    filtered = filtered[filtered["categorie_distance"] == cat]

filtered = sample_df(filtered, max_rows=max_rows)

# KPIs
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Trajets", f"{len(filtered):,}".replace(",", " "))
k2.metric("Distance totale (km)", f"{filtered['distance_km'].sum():,.0f}".replace(",", " "))
k3.metric("CO2 total (kg)", f"{filtered['co2_kg'].sum():,.2f}".replace(",", " "))
k4.metric("CO2 moyen (kg/trajet)", f"{filtered['co2_kg'].mean():.3f}")
k5.metric("Intensité moyenne (kg/km)", f"{filtered['co2_par_km'].mean():.4f}")

tabs = st.tabs([
    "📊 Distributions",
    "🧭 Relations & corrélations",
    "🏆 Tops (routes)",
    "🚂 Par véhicule",
    "🧪 Qualité des données",
    "🧾 Données"
])

with tabs[0]:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Distance — histogramme")
        st.pyplot(fig_hist_with_mean(filtered["distance_km"], "Distribution des distances", "Distance (km)"))
        st.subheader("Distance — CDF")
        st.pyplot(fig_cdf(filtered["distance_km"], "CDF distances", "Distance (km)"))
    with c2:
        st.subheader("CO2 — histogramme")
        st.pyplot(fig_hist_with_mean(filtered["co2_kg"], "Distribution CO2/trajet", "CO2 (kg)"))
        st.subheader("CO2 — CDF")
        st.pyplot(fig_cdf(filtered["co2_kg"], "CDF CO2", "CO2 (kg)"))

    st.subheader("Intensité CO2 (kg/km)")
    st.pyplot(fig_hist_with_mean(filtered["co2_par_km"], "Distribution CO2 par km", "kg CO2 / km"))

with tabs[1]:
    c1, c2 = st.columns([2, 1])
    with c1:
        st.subheader("Distance vs CO2")
        st.pyplot(fig_scatter(filtered))
    with c2:
        st.subheader("Heatmap corrélations")
        st.pyplot(fig_heatmap_corr(filtered))

with tabs[2]:
    st.subheader("Top routes")
    n = st.slider("Top N", 5, 50, 15, 5)
    metric = st.selectbox("Critère", ["co2_sum", "co2_mean", "distance_mean", "trips"], index=0)

    fig, top = fig_top_routes(filtered, n=n, metric=metric)
    st.pyplot(fig)
    st.dataframe(top, width="stretch", height=300)

    st.subheader("CO2 total par catégorie")
    fig2, cat_stats = fig_co2_by_category(filtered)
    st.pyplot(fig2)
    st.dataframe(cat_stats, width="stretch")

with tabs[3]:
    st.subheader("CO2 par véhicule (boxplot)")
    st.pyplot(fig_box_vehicle(filtered, "co2_kg", "CO2 par véhicule (boxplot)", "CO2 (kg)"))

    st.subheader("CO2 par km par véhicule (violin)")
    st.pyplot(fig_violin_vehicle(filtered, "co2_par_km", "Intensité CO2 par véhicule", "kg/km"))

with tabs[4]:
    st.subheader("Qualité des données (contrôle)")
    rep = quality_report(filtered)

    q1, q2, q3, q4 = st.columns(4)
    q1.metric("Lignes", f"{rep['rows']:,}".replace(",", " "))
    q2.metric("Nulls total", rep["nulls_total"])
    q3.metric("Doublons (clé logique)", rep["duplicates"])
    q4.metric("Outliers CO2 (IQR)", rep["outliers_co2_iqr"])

    st.caption(f"Bornes IQR CO2: {rep['co2_iqr_bounds'][0]:.3f} → {rep['co2_iqr_bounds'][1]:.3f}")

    st.subheader("Nulls par colonne")
    st.dataframe(rep["nulls_by_col"].rename("nulls").to_frame(), width="stretch")

with tabs[5]:
    st.subheader("Données (échantillonnées)")
    st.dataframe(filtered, width="stretch", height=420)

    csv = filtered.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Télécharger CSV", data=csv, file_name="datamart_co2_filtered.csv", mime="text/csv")