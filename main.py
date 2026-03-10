import os
import sys
import time
import threading
import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path



# Détection d'exécution dans Docker
def running_in_docker():
    path = '/.dockerenv'
    return os.path.exists(path) or os.environ.get('RUNNING_IN_DOCKER') == '1'

# Force PySpark à utiliser le Python qui lance ce script (donc .venv ou Docker)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
# Pour Docker, on peut ajouter d'autres variables si besoin
if running_in_docker():
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


# Spark cleanup (Windows-friendly)
def hard_stop_spark():
    """Tente de stopper proprement tout Spark restant + nettoie les vars gateway."""
    # Stop SparkSession globale
    try:
        from pyspark.sql import SparkSession
        spark = getattr(SparkSession, "_instantiatedSession", None)
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass
    except Exception:
        pass

    # Stop SparkContext global
    try:
        from pyspark import SparkContext
        sc = getattr(SparkContext, "_active_spark_context", None)
        if sc is not None:
            try:
                sc.stop()
            except Exception:
                pass
    except Exception:
        pass

    # Clean PySpark gateway env vars (sinon Py4J peut faire n'importe quoi au run suivant)
    for k in ("PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET"):
        try:
            os.environ.pop(k, None)
        except Exception:
            pass


# Stats
class PipelineStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.phases = {}
        self.start_time = None
        self.end_time = None

    def start(self):
        self.start_time = datetime.now()

    def end(self):
        self.end_time = datetime.now()

    def add_phase(self, name: str, duration: float, success: bool):
        with self.lock:
            self.phases[name] = {"duration": duration, "success": success}

    def summary(self):
        total = (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        return {"phases": self.phases, "total_duration": round(total, 2)}


def log(message: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    icons = {
        "INFO": "ℹ️", "SUCCESS": "✅", "ERROR": "❌",
        "WARN": "⚠️", "EXTRACT": "📥", "TRANSFORM": "🔄", "LOAD": "💾"
    }
    icon = icons.get(level, "•")
    print(f"[{timestamp}] {icon} {message}")


# Phase 1 : Extraction
def run_extraction_mobility():
    log("MOBILITY - Démarrage...", "EXTRACT")
    start = time.time()
    try:
        from extraction.extraction import extract_mobility_database
        success = extract_mobility_database()
        duration = time.time() - start
        log(f"MOBILITY - {'Terminé' if success else 'Échec'} ({duration:.1f}s)", "SUCCESS" if success else "ERROR")
        return success, duration
    except Exception as e:
        duration = time.time() - start
        log(f"MOBILITY - Erreur: {e}", "ERROR")
        return False, duration


def run_extraction_backontrack():
    log("BACKONTRACK - Démarrage...", "EXTRACT")
    start = time.time()
    try:
        from extraction.extraction import extract_backontrack
        success = extract_backontrack()
        duration = time.time() - start
        log(f"BACKONTRACK - {'Terminé' if success else 'Échec'} ({duration:.1f}s)", "SUCCESS" if success else "ERROR")
        return success, duration
    except Exception as e:
        duration = time.time() - start
        log(f"BACKONTRACK - Erreur: {e}", "ERROR")
        return False, duration


def run_extraction_airports():
    log("AIRPORTS - Démarrage...", "EXTRACT")
    start = time.time()
    try:
        from extraction.extraction import extract_airports
        success = extract_airports()
        duration = time.time() - start
        log(f"AIRPORTS - {'Terminé' if success else 'Échec'} ({duration:.1f}s)", "SUCCESS" if success else "ERROR")
        return success, duration
    except Exception as e:
        duration = time.time() - start
        log(f"AIRPORTS - Erreur: {e}", "ERROR")
        return False, duration


def run_extraction_all_parallel(stats: PipelineStats):
    log("PHASE 1: EXTRACTION PARALLÈLE", "INFO")

    start = time.time()
    results = {}

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(run_extraction_backontrack): "backontrack",
            executor.submit(run_extraction_airports): "airports",
            executor.submit(run_extraction_mobility): "mobility",
        }

        for future in as_completed(futures):
            source = futures[future]
            try:
                success, duration = future.result()
                results[source] = success
                stats.add_phase(f"extract_{source}", duration, success)
            except Exception as e:
                log(f"{source.upper()} - Exception: {e}", "ERROR")
                results[source] = False
                stats.add_phase(f"extract_{source}", 0.0, False)

    total_duration = time.time() - start
    success_count = sum(1 for v in results.values() if v)
    log(f"EXTRACTION - {success_count}/3 sources extraites ({total_duration:.1f}s)", "SUCCESS" if success_count == 3 else "WARN")
    return all(results.values())


def run_extraction_sequential(stats: PipelineStats):
    log("PHASE 1: EXTRACTION SÉQUENTIELLE", "INFO")

    start = time.time()
    ok1, d1 = run_extraction_backontrack()
    stats.add_phase("extract_backontrack", d1, ok1)

    ok2, d2 = run_extraction_airports()
    stats.add_phase("extract_airports", d2, ok2)

    ok3, d3 = run_extraction_mobility()
    stats.add_phase("extract_mobility", d3, ok3)

    return ok1 and ok2 and ok3


# Phase 2 : Transformation (subprocess)
def _transform_entrypoint_args():
    """
    Retourne la commande la plus robuste possible :
    1) si transformation est un package -> python -m transformation.transform
    2) sinon -> python transformation/transform.py
    """
    pkg_init = Path("transformation") / "__init__.py"
    transform_py = Path("transformation") / "transform.py"

    if pkg_init.exists():
        return [sys.executable, "-m", "transformation.transform"]

    # fallback ultra sûr
    return [sys.executable, str(transform_py)]


def run_transformation(stats: PipelineStats):
    log("PHASE 2: TRANSFORMATION (SPARK)", "TRANSFORM")

    start = time.time()
    try:
        hard_stop_spark()

        cmd = _transform_entrypoint_args()
        log(f"Commande transformation: {' '.join(cmd)}", "INFO")

        # capture_output=False pour garder les logs Spark visibles dans ta console
        subprocess.run(cmd, check=True)

        duration = time.time() - start
        stats.add_phase("transformation", duration, True)
        log(f"TRANSFORMATION - Terminé ({duration:.1f}s)", "SUCCESS")
        return True

    except subprocess.CalledProcessError as e:
        duration = time.time() - start
        stats.add_phase("transformation", duration, False)
        log(f"TRANSFORMATION - Échec (code={e.returncode})", "ERROR")
        return False

    except Exception as e:
        duration = time.time() - start
        stats.add_phase("transformation", duration, False)
        log(f"TRANSFORMATION - Erreur: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # au cas où Spark a quand même accroché une session pendant le subprocess
        hard_stop_spark()


# Phase 3 : Load
def run_load(stats: PipelineStats, clean_tables: bool = True):
    log("PHASE 3: CHARGEMENT", "LOAD")

    start = time.time()
    try:
        from load.load_to_db import run_ingestion
        run_ingestion(clean_tables=clean_tables)
        duration = time.time() - start
        stats.add_phase("load", duration, True)
        log(f"CHARGEMENT - Terminé ({duration:.1f}s)", "SUCCESS")
        return True
    except Exception as e:
        duration = time.time() - start
        stats.add_phase("load", duration, False)
        log(f"CHARGEMENT - Erreur: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


# Phase 4 : Analyse
def run_analysis(stats: PipelineStats):
    """
    Génère un rapport d'analyse automatique des résultats.
    """
    log("PHASE 4: ANALYSE DES RÉSULTATS", "INFO")

    start = time.time()
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'analyse'))
        from analyse_resultat import run_analysis as generate_report
        
        success = generate_report()
        
        duration = time.time() - start
        stats.add_phase("analyse", duration, True)
        log(f"ANALYSE - Terminé ({duration:.1f}s)", "SUCCESS")
        return success
        
    except FileNotFoundError as e:
        duration = time.time() - start
        stats.add_phase("analyse", duration, False)
        log(f"ANALYSE - Fichier non trouvé: {e}", "WARN")
        return False
        
    except Exception as e:
        duration = time.time() - start
        stats.add_phase("analyse", duration, False)
        log(f"ANALYSE - Erreur: {e}", "WARN")
        import traceback
        traceback.print_exc()
        return False


# Phase 5 : Visualisation
def run_visualization(stats: PipelineStats):
    """
    Génère les graphiques de visualisation (dashboard).
    """
    log("PHASE 5: GÉNÉRATION DES GRAPHIQUES", "INFO")

    start = time.time()
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'visualization'))
        from dashboard import main as generate_dashboard
        
        success = generate_dashboard()
        
        duration = time.time() - start
        stats.add_phase("visualisation", duration, True if success else False)
        
        if success:
            log(f"VISUALISATION - Terminé ({duration:.1f}s)", "SUCCESS")
        else:
            log(f"VISUALISATION - Échec ({duration:.1f}s)", "WARN")
        
        return success
        
    except ImportError as e:
        duration = time.time() - start
        stats.add_phase("visualisation", duration, False)
        log(f"VISUALISATION - Dépendances manquantes: {e}", "WARN")
        log("   Installer: pip install matplotlib seaborn sqlalchemy", "INFO")
        return False
        
    except Exception as e:
        duration = time.time() - start
        stats.add_phase("visualisation", duration, False)
        log(f"VISUALISATION - Erreur: {e}", "WARN")
        import traceback
        traceback.print_exc()
        return False


# Summary + Pipelines
def print_summary(stats: PipelineStats, success: bool):
    summary = stats.summary()
    print("RÉSUMÉ DU PIPELINE")

    for phase, info in summary["phases"].items():
        status = if info["success"] else "error"
        print(f"   {status} {phase}: {info['duration']:.1f}s")

    print(f"Durée totale: {summary['total_duration']:.1f}s")
    print(f"   {'SUCCÈS' if success else 'ÉCHEC'}")


def run_parallel_pipeline(clean_tables: bool = True):
    stats = PipelineStats()
    stats.start()

    print("PIPELINE ETL PARALLÈLE")
    print(f"Début: {stats.start_time.strftime('%H:%M:%S')}")

    success = True

    if not run_extraction_all_parallel(stats):
        log("Certaines extractions ont échoué, on continue", "WARN")

    hard_stop_spark()

    if not run_transformation(stats):
        log("Transformation échouée, arrêt du pipeline", "ERROR")
        success = False

    if success and not run_load(stats, clean_tables=clean_tables):
        log("Chargement échoué", "ERROR")
        success = False

    # Phase 4 : Analyse (non bloquante si échec)
    if success:
        run_analysis(stats)
        # Phase 5 : Visualisation (non bloquante si échec)
        run_visualization(stats)

    stats.end()
    print_summary(stats, success)
    return success


def run_sequential_pipeline(clean_tables: bool = True):
    stats = PipelineStats()
    stats.start()

    print("PIPELINE ETL SÉQUENTIEL")
    print(f"Début: {stats.start_time.strftime('%H:%M:%S')}")

    success = True

    if not run_extraction_sequential(stats):
        log("Extraction échouée, arrêt du pipeline", "ERROR")
        success = False

    hard_stop_spark()

    if success and not run_transformation(stats):
        log("Transformation échouée, arrêt du pipeline", "ERROR")
        success = False

    if success and not run_load(stats, clean_tables=clean_tables):
        log("Chargement échoué", "ERROR")
        success = False

    # Phase 4 : Analyse (non bloquante si échec)
    if success:
        run_analysis(stats)
        # Phase 5 : Visualisation (non bloquante si échec)
        run_visualization(stats)

    stats.end()
    print_summary(stats, success)
    return success


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline ETL CO2 - Train vs Avion"
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--sequential", action="store_true", help="Mode séquentiel")
    mode_group.add_argument("--parallel", action="store_true", help="Extractions en parallèle (défaut)")
    mode_group.add_argument("--extract", action="store_true", help="Extraction seule")
    mode_group.add_argument("--transform", action="store_true", help="Transformation seule")
    mode_group.add_argument("--load", action="store_true", help="Chargement seul")

    parser.add_argument("--no-clean", action="store_true", help="Ne pas vider les tables avant le chargement")

    args = parser.parse_args()
    clean_tables = not args.no_clean

    stats = PipelineStats()
    stats.start()

    try:
        if args.extract:
            run_extraction_all_parallel(stats)
            stats.end()
            print_summary(stats, True)
            return

        if args.transform:
            ok = run_transformation(stats)
            stats.end()
            print_summary(stats, ok)
            sys.exit(0 if ok else 1)

        if args.load:
            ok = run_load(stats, clean_tables=clean_tables)
            stats.end()
            print_summary(stats, ok)
            sys.exit(0 if ok else 1)

        # pipelines complets
        if args.sequential:
            ok = run_sequential_pipeline(clean_tables=clean_tables)
            sys.exit(0 if ok else 1)

        # défaut = parallel
        ok = run_parallel_pipeline(clean_tables=clean_tables)
        sys.exit(0 if ok else 1)

    except KeyboardInterrupt:
        log("Interruption utilisateur", "WARN")
        sys.exit(1)


if __name__ == "__main__":
    main()