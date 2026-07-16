"""FastAPI dashboard + REST API (no build step; HTML/CSS/JS inlined).

Endpoints
---------
GET  /                       -> HTML dashboard (auto-refreshing)
GET  /api/stats              -> job counts by status + worker/workflow totals
GET  /api/jobs?status=&limit -> list jobs
GET  /api/jobs/{id}          -> job detail
POST /api/jobs               -> enqueue {task, args, kwargs, priority, run_at, max_attempts}
POST /api/jobs/{id}/retry    -> requeue a dead job
GET  /api/deadletter         -> dead-letter queue
GET  /api/workers            -> workers + last heartbeat
GET  /api/workflows          -> workflow runs
GET  /api/workflows/{id}     -> workflow run detail + per-step status
POST /api/workflows          -> submit a workflow {name, steps:[...]}

The dashboard is read-mostly plus enqueue/submit; run a worker pool separately
(``python -m jobq.worker``) to actually execute the jobs it shows.
"""

from __future__ import annotations

import time
from typing import Any

from pydantic import BaseModel

from .config import get_store
from .store.base import JobStore


# NOTE: request models MUST live at module scope. FastAPI resolves endpoint
# annotations via ``get_type_hints`` against module globals; a model defined
# inside ``create_app`` would be invisible there (and, under
# ``from __future__ import annotations``, would be mis-read as a query param).
class EnqueueBody(BaseModel):
    task: str
    args: list[Any] = []
    kwargs: dict[str, Any] = {}
    priority: int = 0
    max_attempts: int = 3
    delay: float = 0.0


class WorkflowBody(BaseModel):
    name: str = "workflow"
    steps: list[dict[str, Any]]


def create_app(database_url: str | None = None):
    try:
        from fastapi import FastAPI, HTTPException
        from fastapi.responses import HTMLResponse
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "The dashboard requires fastapi + uvicorn: pip install fastapi uvicorn"
        ) from exc

    import jobq.tasks  # noqa: F401 - ensure example tasks are importable

    store: JobStore = get_store(database_url)
    store.init_schema()

    app = FastAPI(title="jobq dashboard", version="0.1.0")

    # ---- REST API -------------------------------------------------------
    @app.get("/api/stats")
    def stats() -> dict[str, Any]:
        counts = store.counts_by_status()
        workers = store.list_workers()
        runs = store.list_workflow_runs(limit=1000)
        return {
            "counts": counts,
            "workers": len(workers),
            "workers_alive": sum(1 for w in workers if w["status"] == "alive"),
            "workflows": len(runs),
            "now": time.time(),
        }

    @app.get("/api/jobs")
    def list_jobs(status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return [j.to_dict() for j in store.list_jobs(status=status, limit=limit)]

    @app.get("/api/jobs/{job_id}")
    def get_job(job_id: int) -> dict[str, Any]:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job.to_dict()

    @app.post("/api/jobs")
    def enqueue(body: EnqueueBody) -> dict[str, Any]:
        run_at = None if body.delay == 0 else time.time() + body.delay
        job_id = store.enqueue(
            body.task,
            args=body.args,
            kwargs=body.kwargs,
            priority=body.priority,
            max_attempts=body.max_attempts,
            run_at=run_at,
        )
        return {"id": job_id, "status": "queued", "task": body.task}

    @app.post("/api/jobs/{job_id}/retry")
    def retry_job(job_id: int) -> dict[str, Any]:
        ok = store.requeue_dead(job_id)
        if not ok:
            raise HTTPException(status_code=400, detail="job is not dead / not found")
        return {"id": job_id, "status": "queued"}

    @app.get("/api/deadletter")
    def deadletter(limit: int = 100) -> list[dict[str, Any]]:
        return [j.to_dict() for j in store.dead_letter(limit=limit)]

    @app.get("/api/workers")
    def workers() -> list[dict[str, Any]]:
        return store.list_workers()

    @app.get("/api/workflows")
    def workflows() -> list[dict[str, Any]]:
        return store.list_workflow_runs(limit=200)

    @app.get("/api/workflows/{run_id}")
    def workflow_detail(run_id: int) -> dict[str, Any]:
        run = store.get_workflow_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="workflow run not found")
        run["steps"] = store.list_workflow_steps(run_id)
        return run

    @app.post("/api/workflows")
    def submit_workflow_api(body: WorkflowBody) -> dict[str, Any]:
        from .dag import DagValidationError, CycleError, load_dag, submit_workflow

        try:
            dag = load_dag({"name": body.name, "steps": body.steps})
        except (DagValidationError, CycleError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        run_id = submit_workflow(store, dag)
        return {"run_id": run_id, "name": dag.name, "steps": len(dag.steps)}

    # ---- HTML -----------------------------------------------------------
    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return _HTML

    return app


def serve(host: str = "127.0.0.1", port: int = 8000, database_url: str | None = None) -> None:
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover
        raise ImportError("uvicorn is required to serve: pip install uvicorn") from exc
    app = create_app(database_url)
    uvicorn.run(app, host=host, port=port, log_level="warning")


_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>jobq dashboard</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin:0; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
         background:#0d1117; color:#c9d1d9; }
  header { padding:14px 20px; background:#161b22; border-bottom:1px solid #30363d;
           display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
  header h1 { font-size:16px; margin:0; color:#58a6ff; }
  .pill { padding:2px 8px; border-radius:10px; font-size:12px; border:1px solid #30363d; }
  main { padding:20px; display:grid; grid-template-columns:1fr 1fr; gap:20px; }
  section { background:#161b22; border:1px solid #30363d; border-radius:8px; padding:14px;
            overflow-x:auto; }
  section.full { grid-column:1 / -1; }
  h2 { font-size:13px; margin:0 0 10px; color:#8b949e; text-transform:uppercase;
       letter-spacing:.05em; }
  table { width:100%; border-collapse:collapse; font-size:12px; }
  th,td { text-align:left; padding:5px 8px; border-bottom:1px solid #21262d; white-space:nowrap; }
  th { color:#8b949e; font-weight:600; }
  .s-queued{color:#d29922} .s-running{color:#58a6ff} .s-succeeded{color:#3fb950}
  .s-failed{color:#f0883e} .s-dead{color:#f85149} .s-blocked{color:#8b949e}
  .s-pending{color:#8b949e} .s-alive{color:#3fb950} .s-stopped{color:#8b949e}
  .stat { display:inline-block; margin-right:14px; }
  .stat b { font-size:20px; }
  button { background:#21262d; color:#c9d1d9; border:1px solid #30363d; border-radius:6px;
           padding:3px 8px; cursor:pointer; font-family:inherit; font-size:11px; }
  button:hover { border-color:#58a6ff; }
  .muted { color:#6e7681; font-size:11px; }
  .row-actions { display:flex; gap:6px; }
</style>
</head>
<body>
<header>
  <h1>jobq</h1>
  <span class="pill">job queue &amp; workflow engine</span>
  <span id="clock" class="muted"></span>
  <span style="flex:1"></span>
  <label class="muted"><input type="checkbox" id="auto" checked/> auto-refresh</label>
  <button onclick="refresh()">refresh now</button>
</header>
<main>
  <section class="full">
    <h2>Job states</h2>
    <div id="stats"></div>
  </section>
  <section>
    <h2>Jobs</h2>
    <table><thead><tr><th>id</th><th>task</th><th>status</th><th>att</th>
      <th>prio</th><th>wf</th></tr></thead><tbody id="jobs"></tbody></table>
  </section>
  <section>
    <h2>Workers &amp; heartbeats</h2>
    <table><thead><tr><th>id</th><th>status</th><th>job</th><th>heartbeat age</th>
      </tr></thead><tbody id="workers"></tbody></table>
  </section>
  <section>
    <h2>Dead-letter queue</h2>
    <table><thead><tr><th>id</th><th>task</th><th>att</th><th>error</th><th></th>
      </tr></thead><tbody id="dlq"></tbody></table>
  </section>
  <section>
    <h2>Workflow runs</h2>
    <table><thead><tr><th>id</th><th>name</th><th>status</th><th>steps</th>
      </tr></thead><tbody id="workflows"></tbody></table>
  </section>
</main>
<script>
const $ = id => document.getElementById(id);
const sc = s => `<span class="s-${s}">${s}</span>`;
async function j(u,opt){ const r=await fetch(u,opt); return r.json(); }

async function refresh(){
  try {
    const now = Date.now()/1000;
    const st = await j('/api/stats');
    const c = st.counts;
    $('stats').innerHTML = ['queued','running','succeeded','failed','dead']
      .map(k=>`<span class="stat">${sc(k)}<br><b>${c[k]||0}</b></span>`).join('')
      + `<span class="stat muted">workers<br><b>${st.workers_alive}/${st.workers}</b></span>`
      + `<span class="stat muted">workflows<br><b>${st.workflows}</b></span>`;

    const jobs = await j('/api/jobs?limit=25');
    $('jobs').innerHTML = jobs.map(x=>`<tr><td>#${x.id}</td><td>${x.task}</td>
      <td>${sc(x.status)}</td><td>${x.attempts}/${x.max_attempts}</td>
      <td>${x.priority}</td><td>${x.workflow_run_id??''}</td></tr>`).join('');

    const w = await j('/api/workers');
    $('workers').innerHTML = w.map(x=>{
      const age = x.last_heartbeat ? (now-x.last_heartbeat).toFixed(1)+'s' : '-';
      return `<tr><td>${x.id}</td><td>${sc(x.status)}</td>
        <td>${x.current_job_id??''}</td><td>${age}</td></tr>`;}).join('');

    const dlq = await j('/api/deadletter');
    $('dlq').innerHTML = dlq.map(x=>`<tr><td>#${x.id}</td><td>${x.task}</td>
      <td>${x.attempts}</td><td class="muted">${(x.error||'').slice(0,40)}</td>
      <td><button onclick="retryJob(${x.id})">retry</button></td></tr>`).join('')
      || '<tr><td colspan=5 class="muted">empty</td></tr>';

    const wf = await j('/api/workflows');
    const rows = await Promise.all(wf.map(async r=>{
      const d = await j('/api/workflows/'+r.id);
      const steps = (d.steps||[]).map(s=>`${s.name}:${s.status[0]}`).join(' ');
      return `<tr><td>#${r.id}</td><td>${r.name}</td><td>${sc(r.status)}</td>
        <td class="muted">${steps}</td></tr>`;}));
    $('workflows').innerHTML = rows.join('') ||
      '<tr><td colspan=4 class="muted">none</td></tr>';

    $('clock').textContent = 'updated ' + new Date().toLocaleTimeString();
  } catch(e){ $('clock').textContent = 'error: '+e; }
}
async function retryJob(id){ await j('/api/jobs/'+id+'/retry',{method:'POST'}); refresh(); }
setInterval(()=>{ if($('auto').checked) refresh(); }, 2000);
refresh();
</script>
</body>
</html>
"""


def _main() -> None:  # pragma: no cover
    from .config import EngineConfig

    cfg = EngineConfig.from_env()
    serve(host=cfg.dashboard_host, port=cfg.dashboard_port, database_url=cfg.database_url)


if __name__ == "__main__":  # pragma: no cover
    _main()
