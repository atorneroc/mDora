import csv
import glob
import os
import sys
import time
from datetime import datetime

import requests

# ─── Configuración desde variables de entorno ───
TOKEN = os.environ["GITHUB_TOKEN"]
ORG = os.environ.get("INPUT_ORG", "CCAPITAL-APPS")
TOPIC = os.environ.get("INPUT_TOPIC", "tribu-canal-digital")
INPUT_REPO = os.environ.get("INPUT_REPO", "").strip()
API_URL = os.environ.get("GITHUB_API_URL", "https://api.github.com")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "cycle-time-report")
SUMMARY_FILE = os.environ.get("GITHUB_STEP_SUMMARY", "")
TOTAL_SHARDS = int(os.environ.get("TOTAL_SHARDS", "1") or "1")
_shard_raw = os.environ.get("SHARD_ID", "").strip()
SHARD_ID = int(_shard_raw) if _shard_raw else None  # None = todos los shards

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
                print(f"  ⏳ Rate limit alcanzado. Esperando {wait:.0f}s...")
                time.sleep(wait)
                continue
        return resp


def get_paginated(url):
    """Obtiene todas las páginas de un endpoint paginado de la API de GitHub."""
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
    print(f"Repositorios encontrados: {len(filtered)} con topic '{topic}' (de {len(all_repos)} totales)")
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
    url = f"{API_URL}/repos/{org}/{repo_name}/pulls/{pr_number}/commits?per_page=100"
    return get_paginated(url)


def parse_date(s):
    """Parsea una fecha ISO 8601 con tolerancia."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def minutes_between(d1, d2):
    """Calcula minutos entre dos datetimes."""
    if d1 and d2:
        return round((d2 - d1).total_seconds() / 60)
    return None


# ═══════════════════════════════════════════════════
# Lógica principal
# ═══════════════════════════════════════════════════

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


def process_repos(repos):
    """Procesa una lista de repos y retorna todos los commits con cycle time calculado."""
    all_commits = []

    for repo in repos:
        repo_name = repo["name"]
        print(f"\n{'='*60}")
        print(f"Procesando: {repo_name}")

        # ── Paso 1: feature → develop ──
        develop_prs = get_merged_prs(ORG, repo_name, "develop")
        print(f"  PRs mergeados → develop: {len(develop_prs)}")

        repo_commits = []
        for pr in develop_prs:
            commits = get_pr_commits(ORG, repo_name, pr["number"])
            for c in commits:
                repo_commits.append({
                    "repo_name": repo_name,
                    "commit_sha": c["sha"],
                    "commit_date": c["commit"]["author"]["date"],
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

        # Lookup por SHA (scope: solo este repo)
        sha_lookup = {}
        for info in repo_commits:
            sha_lookup.setdefault(info["commit_sha"], []).append(info)

        # ── Paso 2: develop → qa ──
        qa_prs = get_merged_prs(ORG, repo_name, "qa")
        print(f"  PRs mergeados → qa: {len(qa_prs)}")
        for pr in qa_prs:
            commits = get_pr_commits(ORG, repo_name, pr["number"])
            for c in commits:
                if c["sha"] in sha_lookup:
                    for info in sha_lookup[c["sha"]]:
                        info["pr_dev_qa_id"] = pr["number"]
                        info["merge_to_qa_date"] = pr.get("merged_at")

        # ── Paso 3: qa → main ──
        main_prs = get_merged_prs(ORG, repo_name, "main")
        print(f"  PRs mergeados → main: {len(main_prs)}")
        for pr in main_prs:
            commits = get_pr_commits(ORG, repo_name, pr["number"])
            for c in commits:
                if c["sha"] in sha_lookup:
                    for info in sha_lookup[c["sha"]]:
                        info["pr_qa_main_id"] = pr["number"]
                        info["merge_to_main_date"] = pr.get("merged_at")

        all_commits.extend(repo_commits)

    # ── Calcular Cycle Times ──
    for info in all_commits:
        commit_dt = parse_date(info["commit_date"])
        merged_dev_dt = parse_date(info["merged_at"])
        merged_qa_dt = parse_date(info["merge_to_qa_date"])
        merged_main_dt = parse_date(info["merge_to_main_date"])

        info["ct_commit_to_develop_min"] = minutes_between(commit_dt, merged_dev_dt)
        info["ct_commit_to_qa_min"] = minutes_between(commit_dt, merged_qa_dt)
        info["ct_commit_to_main_min"] = minutes_between(commit_dt, merged_main_dt)

    return all_commits


def main():
    # ── Obtener repositorios ──
    if INPUT_REPO:
        print(f"Modo repo único: {ORG}/{INPUT_REPO}")
        repos = get_repos_by_topic(ORG, TOPIC)
        repos = [r for r in repos if r["name"].lower() == INPUT_REPO.lower()]
        if not repos:
            print(f"Repositorio '{INPUT_REPO}' no encontrado o no tiene topic '{TOPIC}'.")
            write_summary([], [], mode="single", shard_id=1)
            write_csvs([], [])
            return
    else:
        repos = get_repos_by_topic(ORG, TOPIC)

    if not repos:
        print("No se encontraron repositorios. Finalizando.")
        write_summary([], [], mode="single", shard_id=1)
        write_csvs([], [])
        return

    # ── Determinar modo de ejecución ──
    if SHARD_ID is not None:
        # ─── Modo shard individual ───
        print(f"\n🔷 Modo: shard individual ({SHARD_ID}/{TOTAL_SHARDS})")
        if TOTAL_SHARDS > 1 and not INPUT_REPO:
            shard_repos = apply_sharding(repos, SHARD_ID, TOTAL_SHARDS)
        else:
            shard_repos = repos

        commits = process_repos(shard_repos)
        summary = build_summary(commits)
        write_csvs(commits, summary, shard_suffix=f"shard-{SHARD_ID}")

        # Verificar si hay datos de shards anteriores (descargados de ref_run_id)
        shard_dirs = glob.glob(os.path.join(OUTPUT_DIR, "shard-*"))
        if len(shard_dirs) > 1:
            print(f"\n📦 Reconstruyendo consolidado desde {len(shard_dirs)} shards...")
            combined_commits, combined_summary = rebuild_combined_from_shards()
            if combined_commits is not None:
                write_summary(combined_commits, combined_summary, mode="rebuild",
                              shard_id=SHARD_ID, rebuilt_shards=len(shard_dirs))
                print(f"\nFinalizado shard {SHARD_ID}: consolidado con {len(combined_commits)} commits de {len(shard_dirs)} shards.")
            else:
                write_csvs(commits, summary)
                write_summary(commits, summary, mode="single", shard_id=SHARD_ID)
        else:
            write_csvs(commits, summary)
            write_summary(commits, summary, mode="single", shard_id=SHARD_ID)
            print(f"\nFinalizado shard {SHARD_ID}: {len(commits)} commits de {len(shard_repos)} repos.")
    else:
        # ─── Modo todos los shards secuencialmente ───
        print(f"\n🔷 Modo: todos los shards secuencialmente ({TOTAL_SHARDS} shards)")
        combined_commits = []
        failed_shards = []
        succeeded_shards = []

        for sid in range(1, TOTAL_SHARDS + 1):
            print(f"\n{'#'*60}")
            print(f"# SHARD {sid}/{TOTAL_SHARDS}")
            print(f"{'#'*60}")

            try:
                if TOTAL_SHARDS > 1 and not INPUT_REPO:
                    shard_repos = apply_sharding(repos, sid, TOTAL_SHARDS)
                else:
                    shard_repos = repos

                shard_commits = process_repos(shard_repos)
                combined_commits.extend(shard_commits)

                # Guardar CSV intermedio del shard
                shard_summary = build_summary(shard_commits)
                write_csvs(shard_commits, shard_summary, shard_suffix=f"shard-{sid}")
                succeeded_shards.append({"id": sid, "commits": len(shard_commits), "repos": len(shard_repos)})
                print(f"  ✅ Shard {sid} completado: {len(shard_commits)} commits de {len(shard_repos)} repos.")
            except Exception as e:
                failed_shards.append({"id": sid, "error": str(e)})
                print(f"  ❌ Shard {sid} falló: {e}")
                print(f"     Para reprocesar: total_shards={TOTAL_SHARDS}, shard_id={sid}")
                continue

        # Guardar CSV combinado con los shards exitosos
        combined_summary = build_summary(combined_commits)
        write_csvs(combined_commits, combined_summary)
        write_summary(combined_commits, combined_summary, mode="all", shard_id=None,
                      succeeded=succeeded_shards, failed=failed_shards)

        ok = len(succeeded_shards)
        fail = len(failed_shards)
        print(f"\nFinalizado: {ok} shards exitosos, {fail} fallidos, {len(combined_commits)} commits.")

        if failed_shards:
            ids = ", ".join(str(f["id"]) for f in failed_shards)
            run_id = os.environ.get("GITHUB_RUN_ID", "???")
            print(f"⚠️  Shards fallidos: {ids}")
            print(f"   Reprocesar con: total_shards={TOTAL_SHARDS}, shard_id=<id>, ref_run_id={run_id}")
            sys.exit(1)


def build_summary(commits):
    """Agrupa métricas por repositorio."""
    repos = sorted(set(c["repo_name"] for c in commits))
    summary = []
    for repo in repos:
        rc = [c for c in commits if c["repo_name"] == repo]
        reached_qa = [c for c in rc if c["merge_to_qa_date"]]
        reached_main = [c for c in rc if c["merge_to_main_date"]]
        ct_vals = [c["ct_commit_to_main_min"] for c in reached_main if c["ct_commit_to_main_min"] is not None]
        avg_ct = round(sum(ct_vals) / len(ct_vals)) if ct_vals else None
        summary.append({
            "repo": repo,
            "total_commits": len(rc),
            "reached_qa": len(reached_qa),
            "reached_main": len(reached_main),
            "avg_cycle_time_min": avg_ct,
            "avg_cycle_time_hrs": round(avg_ct / 60, 2) if avg_ct else None,
        })
    return summary


def build_branch_summary(commits):
    """Agrupa commits por (repo, rama origen) y calcula cycle time rama → main.

    Para cada rama:
    - first_commit_date: fecha del commit más antiguo en esa rama.
    - merge_to_main_date: fecha del último merge a main que incluye commits de esa rama.
    - cycle_time = merge_to_main_date - first_commit_date.
    """
    from collections import defaultdict

    groups = defaultdict(list)
    for c in commits:
        key = (c["repo_name"], c["source_branch"])
        groups[key].append(c)

    branch_rows = []
    for (repo, branch), grp in sorted(groups.items()):
        commit_dates = [parse_date(c["commit_date"]) for c in grp]
        commit_dates = [d for d in commit_dates if d]
        if not commit_dates:
            continue

        first_commit_date = min(commit_dates)

        # Fecha del último merge a main (máxima entre los commits del grupo)
        main_dates = [parse_date(c["merge_to_main_date"]) for c in grp]
        main_dates = [d for d in main_dates if d]
        last_merge_main = max(main_dates) if main_dates else None

        # Fecha del último merge a qa
        qa_dates = [parse_date(c["merge_to_qa_date"]) for c in grp]
        qa_dates = [d for d in qa_dates if d]
        last_merge_qa = max(qa_dates) if qa_dates else None

        ct_min = minutes_between(first_commit_date, last_merge_main)

        branch_rows.append({
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

    return branch_rows


def load_shard_csv(path):
    """Carga commits desde un CSV de shard, convirtiendo tipos."""
    commits = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for key in ["pr_id", "pr_dev_qa_id", "pr_qa_main_id",
                        "ct_commit_to_develop_min", "ct_commit_to_qa_min", "ct_commit_to_main_min"]:
                val = row.get(key, "")
                if val and val not in ("", "None"):
                    try:
                        row[key] = int(float(val))
                    except (ValueError, TypeError):
                        row[key] = None
                else:
                    row[key] = None
            for key in ["merged_at", "merge_to_qa_date", "merge_to_main_date"]:
                if not row.get(key) or row[key] in ("", "None"):
                    row[key] = None
            commits.append(row)
    return commits


def rebuild_combined_from_shards():
    """Reconstruye CSVs consolidados desde todas las carpetas shard-*/."""
    shard_dirs = sorted(glob.glob(os.path.join(OUTPUT_DIR, "shard-*")))
    if not shard_dirs:
        return None, None

    all_commits = []
    loaded_shards = []
    for shard_dir in shard_dirs:
        detail_path = os.path.join(shard_dir, "cycle-time-detail.csv")
        if os.path.exists(detail_path):
            shard_commits = load_shard_csv(detail_path)
            shard_name = os.path.basename(shard_dir)
            print(f"  Cargando {shard_name}: {len(shard_commits)} commits")
            all_commits.extend(shard_commits)
            loaded_shards.append(shard_name)

    if not all_commits:
        return None, None

    combined_summary = build_summary(all_commits)
    write_csvs(all_commits, combined_summary)
    print(f"  Consolidado reconstruido: {len(all_commits)} commits de {len(loaded_shards)} shards")
    return all_commits, combined_summary


def write_csvs(commits, summary, shard_suffix=None):
    """Exporta CSVs de detalle y resumen."""
    if shard_suffix:
        out_dir = os.path.join(OUTPUT_DIR, shard_suffix)
    else:
        out_dir = OUTPUT_DIR
    os.makedirs(out_dir, exist_ok=True)

    # CSV de detalle
    detail_path = os.path.join(out_dir, "cycle-time-detail.csv")
    detail_cols = [
        "repo_name", "commit_sha", "commit_date", "pr_id", "merged_at",
        "source_branch", "pr_dev_qa_id", "merge_to_qa_date",
        "pr_qa_main_id", "merge_to_main_date",
        "ct_commit_to_develop_min", "ct_commit_to_qa_min", "ct_commit_to_main_min",
    ]
    with open(detail_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=detail_cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(commits)

    # CSV de resumen
    summary_path = os.path.join(out_dir, "cycle-time-summary.csv")
    summary_cols = [
        "repo", "total_commits", "reached_qa", "reached_main",
        "avg_cycle_time_min", "avg_cycle_time_hrs",
    ]
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=summary_cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(summary)

    # CSV por rama (agrupado: 1 línea por source_branch)
    branch_rows = build_branch_summary(commits)
    branch_path = os.path.join(out_dir, "cycle-time-by-branch.csv")
    branch_cols = [
        "repo_name", "source_branch", "total_commits",
        "first_commit_date", "merge_to_qa_date", "merge_to_main_date",
        "cycle_time_min", "cycle_time_hrs", "cycle_time_days",
    ]
    with open(branch_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=branch_cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(branch_rows)

    print(f"  CSVs exportados en {OUTPUT_DIR}/")


def write_summary(commits, summary, mode="single", shard_id=None,
                   succeeded=None, failed=None, rebuilt_shards=None):
    """Escribe el Job Summary de GitHub Actions."""
    if not SUMMARY_FILE:
        return

    lines = []
    lines.append("## 📊 Reporte de Cycle Time\n\n")
    lines.append(f"📅 Generado: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}  \n")
    lines.append(f"🏢 Organización: **{ORG}** | Topic: **{TOPIC}**  \n")
    if INPUT_REPO:
        lines.append(f"📦 Repositorio: **{INPUT_REPO}**  \n")
    if mode == "single" and TOTAL_SHARDS > 1:
        lines.append(f"🔀 Shard: **{shard_id}/{TOTAL_SHARDS}**  \n")
    elif mode == "rebuild":
        lines.append(f"🔀 Shard reprocesado: **{shard_id}** | Consolidado desde **{rebuilt_shards}/{TOTAL_SHARDS}** shards  \n")
    elif mode == "all" and TOTAL_SHARDS > 1:
        ok = len(succeeded or [])
        fail = len(failed or [])
        lines.append(f"🔀 Shards: **{ok} exitosos** | **{fail} fallidos** de **{TOTAL_SHARDS}** totales  \n")
    lines.append(f"📈 **{len(commits)}** commits rastreados | **{len(summary)}** repositorios\n\n")

    # ── Tabla de estado de shards (solo modo all con >1 shard) ──
    if mode == "all" and TOTAL_SHARDS > 1 and (succeeded or failed):
        lines.append("### 🔀 Estado de shards\n\n")
        lines.append("| Shard | Estado | Commits | Repos | Error |\n")
        lines.append("|-------|--------|---------|-------|-------|\n")
        shard_map = {}
        for s in (succeeded or []):
            shard_map[s["id"]] = {"status": "✅", "commits": s["commits"], "repos": s["repos"], "error": "-"}
        for f in (failed or []):
            shard_map[f["id"]] = {"status": "❌", "commits": "-", "repos": "-", "error": f["error"][:80]}
        for sid in sorted(shard_map.keys()):
            s = shard_map[sid]
            lines.append(f"| {sid} | {s['status']} | {s['commits']} | {s['repos']} | {s['error']} |\n")
        lines.append("\n")
        if failed:
            ids = ", ".join(str(f["id"]) for f in failed)
            run_id = os.environ.get("GITHUB_RUN_ID", "???")
            lines.append(f"> ⚠️ Para reprocesar shards fallidos: `total_shards={TOTAL_SHARDS}`, `shard_id=` **{ids}**, `ref_run_id={run_id}`\n\n")

    if not summary:
        lines.append("⚠️ No se encontraron datos.\n")
    else:
        # Stats globales
        all_ct = [c["ct_commit_to_main_min"] for c in commits if c.get("ct_commit_to_main_min") is not None]
        global_avg = round(sum(all_ct) / len(all_ct)) if all_ct else None
        total_reached = sum(s["reached_main"] for s in summary)

        avg_str = f"{global_avg} min ({round(global_avg / 60, 2)} hrs)" if global_avg else "N/A"
        lines.append(f"**Cycle Time promedio global (commit → main):** {avg_str}  \n")
        lines.append(f"**Commits en producción:** {total_reached} de {len(commits)}\n\n")

        # Tabla resumen por repo
        lines.append("### 📋 Resumen por repositorio\n\n")
        lines.append("| Repositorio | Commits | → QA | → Main | CT prom. (min) | CT prom. (hrs) |\n")
        lines.append("|-------------|---------|------|--------|----------------|----------------|\n")
        for s in summary:
            ct_min = str(s["avg_cycle_time_min"]) if s["avg_cycle_time_min"] is not None else "N/A"
            ct_hrs = str(s["avg_cycle_time_hrs"]) if s["avg_cycle_time_hrs"] is not None else "N/A"
            lines.append(
                f"| {s['repo']} | {s['total_commits']} | {s['reached_qa']} "
                f"| {s['reached_main']} | {ct_min} | {ct_hrs} |\n"
            )

        # ── Tabla resumen por rama (cycle time agrupado) ──
        branch_rows = build_branch_summary(commits)
        branches_with_ct = [b for b in branch_rows if b["cycle_time_min"] is not None]
        if branches_with_ct:
            branch_ct_vals = [b["cycle_time_min"] for b in branches_with_ct]
            branch_avg = round(sum(branch_ct_vals) / len(branch_ct_vals))
            branch_avg_str = f"{branch_avg} min ({round(branch_avg / 60, 2)} hrs)"

            lines.append(f"\n### 🌿 Cycle Time por rama origen\n\n")
            lines.append(f"**Ramas que llegaron a main:** {len(branches_with_ct)} de {len(branch_rows)}  \n")
            lines.append(f"**Cycle Time promedio por rama (primer commit → main):** {branch_avg_str}\n\n")
            lines.append("| Repositorio | Rama origen | Commits | Primer commit | Merge a main | CT (min) | CT (hrs) | CT (días) |\n")
            lines.append("|-------------|-------------|---------|---------------|--------------|----------|----------|-----------|\n")
            for b in branches_with_ct:
                fc = b["first_commit_date"][:10] if b["first_commit_date"] else "N/A"
                mm = b["merge_to_main_date"][:10] if b["merge_to_main_date"] else "N/A"
                lines.append(
                    f"| {b['repo_name']} | {b['source_branch']} | {b['total_commits']} "
                    f"| {fc} | {mm} "
                    f"| {b['cycle_time_min']} | {b['cycle_time_hrs']} | {b['cycle_time_days']} |\n"
                )

    with open(SUMMARY_FILE, "a", encoding="utf-8") as f:
        f.writelines(lines)


if __name__ == "__main__":
    main()
