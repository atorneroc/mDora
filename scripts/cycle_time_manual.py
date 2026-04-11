"""
Script manual para calcular Cycle Time.
Uso: python scripts/cycle_time_manual.py

Misma lógica que cycle_time.py (automatizado), pero diseñado para
ejecución local con un token clásico (PAT).

Soporta sharding para manejar volúmenes grandes (+1500 repos).
"""

import requests
import time
import pandas as pd
from datetime import datetime

# ═══════════════════════════════════════════════════
# CONFIGURACIÓN — Editar antes de ejecutar
# ═══════════════════════════════════════════════════

ORG = "CCAPITAL-APPS"
TOPIC = "tribu-canal-digital"
API_URL = "https://api.github.com"

# Token clásico de GitHub (PAT)
TOKEN = "token"  # Reemplaza con tu token

# Sharding: para dividir la carga en ejecuciones separadas.
# TOTAL_SHARDS = 1 y SHARD_ID = 1 → sin sharding (procesa todo).
# TOTAL_SHARDS = 5 y SHARD_ID = 2 → procesa solo el bloque 2 de 5.
TOTAL_SHARDS = 20
SHARD_ID = 20

OUT_FILE = f"prs_merged_main_shard_{SHARD_ID}_of_{TOTAL_SHARDS}.csv"
OUT_BRANCH_FILE = f"prs_merged_main_by_branch_shard_{SHARD_ID}_of_{TOTAL_SHARDS}.csv"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}


# ═══════════════════════════════════════════════════
# Funciones de acceso a la API con paginación y
# manejo de rate limit
# ═══════════════════════════════════════════════════

def request_with_retry(url):
    """GET con reintentos automáticos ante rate limit."""
    while True:
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code == 403:
            remaining = int(resp.headers.get("X-RateLimit-Remaining", 1))
            if remaining == 0:
                reset_ts = int(resp.headers.get("X-RateLimit-Reset", 0))
                wait = max(reset_ts - time.time(), 1) + 1
                reset_time_str = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.gmtime(reset_ts)
                )
                print(
                    f"  ⏳ Rate limit alcanzado. "
                    f"Esperando {wait:.0f}s hasta {reset_time_str}..."
                )
                time.sleep(wait)
                continue
        return resp


def get_paginated(url):
    """Obtiene TODAS las páginas de un endpoint paginado de la API de GitHub."""
    items = []
    while url:
        resp = request_with_retry(url)
        if resp.status_code != 200:
            print(f"  Error {resp.status_code}: {url}")
            break
        data = resp.json()
        if isinstance(data, list):
            items.extend(data)
        else:
            break
        # Parsear Link header para la siguiente página
        url = None
        link_header = resp.headers.get("Link", "")
        for part in link_header.split(","):
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
                break
    return items


# ═══════════════════════════════════════════════════
# Funciones de negocio
# ═══════════════════════════════════════════════════

def get_repos_by_topic(org, topic):
    """Obtiene todos los repositorios de una org filtrados por topic."""
    url = f"{API_URL}/orgs/{org}/repos?per_page=100"
    all_repos = get_paginated(url)
    filtered = [r for r in all_repos if topic in r.get("topics", [])]
    print(
        f"Repositorios encontrados: {len(filtered)} con topic "
        f"'{topic}' (de {len(all_repos)} totales)"
    )
    return filtered


def get_merged_prs(org, repo_name, base_branch):
    """Obtiene todos los PRs mergeados hacia una rama base."""
    url = (
        f"{API_URL}/repos/{org}/{repo_name}/pulls"
        f"?base={base_branch}&state=closed&per_page=100"
        f"&sort=updated&direction=desc"
    )
    prs = get_paginated(url)
    return [pr for pr in prs if pr.get("merged_at")]


def get_pr_commits(org, repo_name, pr_number):
    """Obtiene todos los commits de un PR (paginado)."""
    url = (
        f"{API_URL}/repos/{org}/{repo_name}/pulls"
        f"/{pr_number}/commits?per_page=100"
    )
    return get_paginated(url)


def apply_sharding(repos, shard_id, total_shards):
    """Selecciona el subconjunto de repos para un shard específico."""
    repos = sorted(repos, key=lambda r: r["name"].lower())
    total = len(repos)
    chunk = total // total_shards
    remainder = total % total_shards
    if shard_id <= remainder:
        start = (shard_id - 1) * (chunk + 1)
        end = start + chunk + 1
    else:
        start = remainder * (chunk + 1) + (shard_id - 1 - remainder) * chunk
        end = start + chunk
    selected = repos[start:end]
    print(f"  Shard {shard_id}/{total_shards}: repos {start + 1}-{end} de {total}")
    return selected


def parse_date(s):
    """Parsea una fecha ISO 8601 con tolerancia."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def build_branch_summary(commits_info):
    """Agrupa commits por (repo, rama origen) y calcula cycle time rama → main.

    Para cada rama:
    - first_commit_date: fecha del commit más antiguo.
    - merge_to_main_date: fecha del último merge a main que incluye commits de esa rama.
    - cycle_time = merge_to_main_date - first_commit_date.
    """
    from collections import defaultdict

    groups = defaultdict(list)
    for c in commits_info:
        key = (c["repo_name"], c["source_branch"])
        groups[key].append(c)

    rows = []
    for (repo, branch), grp in sorted(groups.items()):
        commit_dates = [parse_date(c["commit_date"]) for c in grp]
        commit_dates = [d for d in commit_dates if d]
        if not commit_dates:
            continue

        first_commit_date = min(commit_dates)

        main_dates = [parse_date(c["merge_to_main_date"]) for c in grp]
        main_dates = [d for d in main_dates if d]
        last_merge_main = max(main_dates) if main_dates else None

        qa_dates = [parse_date(c["merge_to_qa_date"]) for c in grp]
        qa_dates = [d for d in qa_dates if d]
        last_merge_qa = max(qa_dates) if qa_dates else None

        if first_commit_date and last_merge_main:
            ct_min = round((last_merge_main - first_commit_date).total_seconds() / 60)
        else:
            ct_min = None

        rows.append({
            "repo_name": repo,
            "source_branch": branch,
            "total_commits": len(grp),
            "first_commit_date": first_commit_date.isoformat() if first_commit_date else None,
            "merge_to_qa_date": last_merge_qa.isoformat() if last_merge_qa else None,
            "merge_to_main_date": last_merge_main.isoformat() if last_merge_main else None,
            "cycle_time_min": ct_min,
            "cycle_time_hrs": round(ct_min / 60, 2) if ct_min else None,
            "cycle_time_days": round(ct_min / 1440, 2) if ct_min else None,
        })

    return rows


# ═══════════════════════════════════════════════════
# Lógica principal
# ═══════════════════════════════════════════════════

def main():
    # Paso 1: Obtener repositorios filtrados por topic
    repos = get_repos_by_topic(ORG, TOPIC)
    if not repos:
        print("No se encontraron repositorios. Finalizando.")
        return

    # Aplicar sharding
    if TOTAL_SHARDS > 1:
        repos = apply_sharding(repos, SHARD_ID, TOTAL_SHARDS)

    commits_info = []

    for repo in repos:
        repo_name = repo["name"]
        print(f"\n{'='*60}")
        print(f"Procesando repositorio: {repo_name}")

        # ── Paso 2: PRs mergeados feature → develop ──
        develop_prs = get_merged_prs(ORG, repo_name, "develop")
        print(f"  PRs mergeados → develop: {len(develop_prs)}")

        repo_commits = []
        for pr in develop_prs:
            pr_commits = get_pr_commits(ORG, repo_name, pr["number"])
            for commit in pr_commits:
                repo_commits.append({
                    "repo_name": repo_name,
                    "commit_sha": commit["sha"],
                    "commit_date": commit["commit"]["author"]["date"],
                    "pr_id": pr["number"],
                    "merged_at": pr.get("merged_at"),
                    "source_branch": pr["head"]["ref"],
                    "pr_dev_qa_id": None,
                    "merge_to_qa_date": None,
                    "pr_qa_main_id": None,
                    "merge_to_main_date": None,
                })

        if not repo_commits:
            continue

        # Lookup por SHA (scope: solo este repo, O(1) en vez de O(n))
        sha_lookup = {}
        for info in repo_commits:
            sha_lookup.setdefault(info["commit_sha"], []).append(info)

        # ── Paso 3: PRs develop → qa ──
        qa_prs = get_merged_prs(ORG, repo_name, "qa")
        print(f"  PRs mergeados → qa: {len(qa_prs)}")
        for pr in qa_prs:
            pr_commits = get_pr_commits(ORG, repo_name, pr["number"])
            for commit in pr_commits:
                if commit["sha"] in sha_lookup:
                    for info in sha_lookup[commit["sha"]]:
                        info["pr_dev_qa_id"] = pr["number"]
                        info["merge_to_qa_date"] = pr.get("merged_at")

        # ── Paso 4: PRs qa → main ──
        main_prs = get_merged_prs(ORG, repo_name, "main")
        print(f"  PRs mergeados → main: {len(main_prs)}")
        for pr in main_prs:
            pr_commits = get_pr_commits(ORG, repo_name, pr["number"])
            for commit in pr_commits:
                if commit["sha"] in sha_lookup:
                    for info in sha_lookup[commit["sha"]]:
                        info["pr_qa_main_id"] = pr["number"]
                        info["merge_to_main_date"] = pr.get("merged_at")

        commits_info.extend(repo_commits)

    # ── Exportar CSV de detalle (por commit) ──
    df = pd.DataFrame(
        commits_info,
        columns=[
            "repo_name", "commit_sha", "commit_date",
            "pr_id", "merged_at", "source_branch",
            "pr_dev_qa_id", "merge_to_qa_date",
            "pr_qa_main_id", "merge_to_main_date",
        ],
    )

    df.to_csv(OUT_FILE, index=False)
    print(f"\nExportados {len(df)} commits a {OUT_FILE}")

    # ── Exportar CSV por rama (agrupado) ──
    branch_rows = build_branch_summary(commits_info)
    df_branch = pd.DataFrame(
        branch_rows,
        columns=[
            "repo_name", "source_branch", "total_commits",
            "first_commit_date", "merge_to_qa_date", "merge_to_main_date",
            "cycle_time_min", "cycle_time_hrs", "cycle_time_days",
        ],
    )

    df_branch.to_csv(OUT_BRANCH_FILE, index=False)
    branches_with_ct = df_branch[df_branch["cycle_time_min"].notna()]
    print(f"Exportadas {len(df_branch)} ramas ({len(branches_with_ct)} con CT) a {OUT_BRANCH_FILE}")


if __name__ == "__main__":
    main()
