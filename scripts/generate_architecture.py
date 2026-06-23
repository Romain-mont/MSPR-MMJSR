import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

fig, ax = plt.subplots(1, 1, figsize=(14, 10))
ax.set_xlim(0, 14)
ax.set_ylim(0, 10)
ax.axis('off')
fig.patch.set_facecolor('#F8F9FA')

# ── Couleurs ──────────────────────────────────────────────────────────────────
C_USER    = '#E8F4F8'
C_FRONT   = '#2E6DA8'
C_API     = '#1A3A6E'
C_DB      = '#27AE60'
C_ML      = '#8E44AD'
C_PROM    = '#E87C1E'
C_GRAFANA = '#E87C1E'
C_PGADMIN = '#336791'
C_WHITE   = 'white'
C_BORDER  = '#CCCCCC'

def box(ax, x, y, w, h, label, sublabel, bg, fg='white', radius=0.3):
    rect = FancyBboxPatch((x, y), w, h,
                          boxstyle=f"round,pad=0.05,rounding_size={radius}",
                          facecolor=bg, edgecolor=C_BORDER, linewidth=1.5,
                          zorder=3)
    ax.add_patch(rect)
    ax.text(x + w/2, y + h/2 + 0.12, label,
            ha='center', va='center', fontsize=11, fontweight='bold',
            color=fg, zorder=4)
    if sublabel:
        ax.text(x + w/2, y + h/2 - 0.22, sublabel,
                ha='center', va='center', fontsize=9,
                color=fg, alpha=0.85, zorder=4)

def arrow(ax, x1, y1, x2, y2, label='', color='#555555'):
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle='->', color=color,
                                lw=1.8, connectionstyle='arc3,rad=0.0'),
                zorder=2)
    if label:
        mx, my = (x1+x2)/2, (y1+y2)/2
        ax.text(mx + 0.15, my, label, fontsize=8, color=color,
                ha='left', va='center', style='italic', zorder=5)

# ── Titre ─────────────────────────────────────────────────────────────────────
ax.text(7, 9.6, 'ObRail Europe — Architecture des services',
        ha='center', va='center', fontsize=15, fontweight='bold',
        color='#1A3A6E')
ax.text(7, 9.25, 'Docker Compose · 7 services conteneurisés',
        ha='center', va='center', fontsize=10, color='#555555')

# ── Utilisateur ───────────────────────────────────────────────────────────────
box(ax, 4.5, 8.2, 5, 0.7, 'Navigateur', 'http://localhost', C_USER, fg='#1A3A6E')

# ── Frontend ──────────────────────────────────────────────────────────────────
box(ax, 4.5, 6.9, 5, 0.9, 'Frontend  React 19 + Nginx', 'port 80', C_FRONT)

# ── API ───────────────────────────────────────────────────────────────────────
box(ax, 4.5, 5.5, 5, 0.9, 'API  FastAPI  Python 3.11', 'port 8000  ·  /health /trajets /predict /metrics', C_API)

# ── PostgreSQL ────────────────────────────────────────────────────────────────
box(ax, 1.0, 3.8, 3.2, 0.9, 'PostgreSQL 16', 'port 5432', C_DB)

# ── Modèles ML ────────────────────────────────────────────────────────────────
box(ax, 9.8, 3.8, 3.2, 0.9, 'Modeles ML', 'M1 · M2 · .joblib', C_ML)

# ── Prometheus ────────────────────────────────────────────────────────────────
box(ax, 1.0, 2.0, 3.2, 0.9, 'Prometheus', 'port 9090', C_PROM)

# ── Grafana ───────────────────────────────────────────────────────────────────
box(ax, 4.9, 2.0, 3.2, 0.9, 'Grafana', 'port 3000', C_GRAFANA)

# ── pgAdmin ───────────────────────────────────────────────────────────────────
box(ax, 8.8, 2.0, 3.2, 0.9, 'pgAdmin 4', 'port 5050', C_PGADMIN)

# ── Flèches ───────────────────────────────────────────────────────────────────
# Navigateur → Frontend
arrow(ax, 7.0, 8.2, 7.0, 7.8, 'HTTP', '#2E6DA8')

# Frontend → API
arrow(ax, 7.0, 6.9, 7.0, 6.4, 'fetch() REST', '#1A3A6E')

# API → PostgreSQL
arrow(ax, 5.5, 5.5, 2.8, 4.7, 'SQL', '#27AE60')

# API → Modèles ML
arrow(ax, 8.5, 5.5, 10.6, 4.7, 'predict()', '#8E44AD')

# PostgreSQL → Prometheus (scrape /metrics via API)
arrow(ax, 2.6, 3.8, 2.6, 2.9, 'scrape /metrics  15s', '#E87C1E')

# Prometheus → Grafana
arrow(ax, 4.2, 2.45, 4.9, 2.45, 'PromQL', '#E87C1E')

# PostgreSQL → pgAdmin
arrow(ax, 3.2, 4.25, 9.5, 2.7, 'SQL admin', '#336791')

# ── Légende ───────────────────────────────────────────────────────────────────
legend_items = [
    mpatches.Patch(color=C_FRONT,   label='Frontend'),
    mpatches.Patch(color=C_API,     label='Backend API'),
    mpatches.Patch(color=C_DB,      label='Base de données'),
    mpatches.Patch(color=C_ML,      label='Modèles ML'),
    mpatches.Patch(color=C_PROM,    label='Monitoring'),
    mpatches.Patch(color=C_PGADMIN, label='Administration'),
]
ax.legend(handles=legend_items, loc='lower right', fontsize=9,
          framealpha=0.9, title='Services', title_fontsize=9)

# ── Note ──────────────────────────────────────────────────────────────────────
ax.text(7, 0.3, 'Tous les services sont orchestrés via docker-compose.yml · Réseau interne Docker',
        ha='center', va='center', fontsize=8, color='#888888', style='italic')

plt.tight_layout()
out = '/Users/montagnonromain/Dossier perso Locaux/epsi-cours/MSPR/docs/fig_architecture.png'
plt.savefig(out, dpi=180, bbox_inches='tight', facecolor=fig.get_facecolor())
print(f'Généré : {out}')
