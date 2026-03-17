import json
import os
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
from pymongo import MongoClient


BASE_DIR = Path(__file__).resolve().parent
RUNS_DIR = BASE_DIR / "runs"
JOBS_DIR = RUNS_DIR / "jobs"
HOST = os.getenv("SCRAPER_API_HOST", "0.0.0.0")
PORT = int(os.getenv("PORT") or os.getenv("SCRAPER_API_PORT", "8000"))
MAX_LOG_LINES = 400
JOBS = {}
JOBS_LOCK = threading.Lock()
DEFAULT_MONGO_URI = (
    "mongodb+srv://alishakhan8488_db_user:DaVHn9goL8STNzNs@cluster0.nkmbpqt.mongodb.net/"
    "studentcap?retryWrites=true&w=majority"
)
JOB_MONGO_URI = (
    os.getenv("SCRAPER_JOB_MONGO_URI", "").strip()
    or os.getenv("MONGO_URI", "").strip()
    or DEFAULT_MONGO_URI
)
JOB_MONGO_DB = os.getenv("SCRAPER_JOB_MONGO_DB", "").strip() or os.getenv("MONGO_DB", "studentcap").strip()
JOB_MONGO_COLLECTION = os.getenv("SCRAPER_JOB_MONGO_COLLECTION", "scraper_jobs").strip()
JOB_STORE_WARNED = False


def _env_int(name, default):
    try:
        return max(1, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


MAX_CONCURRENT_JOBS = _env_int(
    "SCRAPER_MAX_CONCURRENT_JOBS",
    1 if os.getenv("RENDER", "").strip().lower() == "true" else 2,
)
JOB_SEMAPHORE = threading.Semaphore(MAX_CONCURRENT_JOBS)


def _resolve_python_executable():
    venv_python = BASE_DIR / "venv" / "Scripts" / "python.exe"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


PYTHON_EXECUTABLE = _resolve_python_executable()


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _slugify(value):
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in (value or ""))
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "job"


def _normalize_task_name(task_name):
    key = (task_name or "").strip().lower().replace("\\", "/").rsplit("/", 1)[-1]
    aliases = {
        "scraper_basic_college_course": "scraper_basic_college_course",
        "scraper_basic_college_course.py": "scraper_basic_college_course",
        "scraper_basic_colege_course": "scraper_basic_college_course",
        "scraper_basic_colege_course.py": "scraper_basic_college_course",
        "scraper_course": "scraper_course",
        "scraper_course.py": "scraper_course",
        "scraper_college": "scraper_college",
        "scraper_college.py": "scraper_college",
        "change_profile": "change_profile",
        "change_profile.py": "change_profile",
        "replace_collegedunia_text": "replace_collegedunia_text",
        "replace_collegedunia_text.py": "replace_collegedunia_text",
    }
    return aliases.get(key, key)


def _ensure_dict_payload(payload):
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ValueError("Request body must be a JSON object.")
    return payload


def _require_url(payload):
    url = str(payload.get("url", "")).strip()
    if not url:
        raise ValueError("`url` is required for this task.")
    return url


def _append_int_arg(command, payload, payload_key, cli_flag):
    value = payload.get(payload_key)
    if value is None or value == "":
        return
    command.extend([cli_flag, str(int(value))])


def _append_str_arg(command, payload, payload_key, cli_flag):
    value = str(payload.get(payload_key, "")).strip()
    if value:
        command.extend([cli_flag, value])


def _append_bool_flag(command, payload, payload_key, cli_flag):
    if payload.get(payload_key):
        command.append(cli_flag)


def _append_headless_args(command, payload):
    if "headless" not in payload:
        if _should_run_headless(payload):
            command.append("--headless")
        return

    if bool(payload.get("headless")):
        command.append("--headless")
    else:
        command.append("--headed")


def _should_run_headless(payload):
    if "headless" in payload:
        return bool(payload.get("headless"))

    default_headless = os.getenv("SCRAPER_DEFAULT_HEADLESS", "").strip().lower()
    if default_headless in {"1", "true", "yes", "on"}:
        return True

    return os.getenv("RENDER", "").strip().lower() == "true"


def _append_string_list_arg(command, payload, payload_key, cli_flag):
    values = payload.get(payload_key)
    if values is None:
        return
    if not isinstance(values, list) or not all(str(item).strip() for item in values):
        raise ValueError(f"`{payload_key}` must be a non-empty string array.")
    command.append(cli_flag)
    command.extend(str(item).strip() for item in values)


def _build_output_path(task_name):
    RUNS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    filename = f"{_slugify(task_name)}_{timestamp}_{suffix}.json"
    return str((RUNS_DIR / filename).resolve())


def _build_basic_course_command(payload, output_file):
    url = _require_url(payload)
    command = [
        PYTHON_EXECUTABLE,
        "scraper_basic_college_course.py",
        "--url",
        url,
        "--output-file",
        output_file,
    ]
    _append_headless_args(command, payload)
    _append_int_arg(command, payload, "slow_mo", "--slow-mo")
    _append_int_arg(command, payload, "limit_courses", "--limit-courses")
    _append_int_arg(command, payload, "limit_sub_courses", "--limit-sub-courses")
    _append_int_arg(command, payload, "max_sections_per_course", "--max-sections-per-course")

    if payload.get("fetch_course_detail") is False:
        command.append("--skip-course-detail")
    if payload.get("fetch_sub_course_detail") is False:
        command.append("--skip-sub-course-detail")

    return command


def _build_course_command(payload, output_file):
    url = _require_url(payload)
    command = [
        PYTHON_EXECUTABLE,
        "scraper_course.py",
        "--url",
        url,
        "--output-file",
        output_file,
    ]
    _append_headless_args(command, payload)
    _append_int_arg(command, payload, "stream_limit", "--stream-limit")
    _append_int_arg(command, payload, "course_limit", "--course-limit")
    return command


def _build_college_command(payload, output_file):
    url = _require_url(payload)
    command = [
        PYTHON_EXECUTABLE,
        "scraper_college.py",
        "--url",
        url,
        "--output-file",
        output_file,
    ]
    _append_headless_args(command, payload)
    return command


def _build_change_profile_command(payload, _output_file):
    command = [PYTHON_EXECUTABLE, "change_profile.py"]
    _append_str_arg(command, payload, "config_script", "--config-script")
    _append_str_arg(command, payload, "mongo_uri", "--mongo-uri")
    _append_str_arg(command, payload, "db", "--db")
    _append_str_arg(command, payload, "collection", "--collection")
    _append_int_arg(command, payload, "batch_size", "--batch-size")
    _append_int_arg(command, payload, "limit", "--limit")
    _append_bool_flag(command, payload, "dry_run", "--dry-run")
    return command


def _build_replace_text_command(payload, _output_file):
    command = [PYTHON_EXECUTABLE, "replace_collegedunia_text.py"]
    _append_str_arg(command, payload, "mongo_uri", "--mongo-uri")
    _append_str_arg(command, payload, "db", "--db")
    _append_str_arg(command, payload, "source_text", "--source-text")
    _append_str_arg(command, payload, "target_text", "--target-text")
    _append_int_arg(command, payload, "batch_size", "--batch-size")
    _append_int_arg(command, payload, "limit", "--limit")
    _append_string_list_arg(command, payload, "collections", "--collections")
    _append_bool_flag(command, payload, "dry_run", "--dry-run")
    _append_bool_flag(command, payload, "skip_url_repair", "--skip-url-repair")
    return command


TASKS = {
    "scraper_basic_college_course": {
        "builder": _build_basic_course_command,
        "requires_url": True,
        "uses_output_file": True,
        "description": "Runs scraper_basic_college_course.py with its own URL.",
    },
    "scraper_course": {
        "builder": _build_course_command,
        "requires_url": True,
        "uses_output_file": True,
        "description": "Runs scraper_course.py with a courses or stream URL.",
    },
    "scraper_college": {
        "builder": _build_college_command,
        "requires_url": True,
        "uses_output_file": True,
        "description": "Runs scraper_college.py with a college or university URL.",
    },
    "change_profile": {
        "builder": _build_change_profile_command,
        "requires_url": False,
        "uses_output_file": False,
        "description": "Runs change_profile.py against MongoDB documents.",
    },
    "replace_collegedunia_text": {
        "builder": _build_replace_text_command,
        "requires_url": False,
        "uses_output_file": False,
        "description": "Runs replace_collegedunia_text.py against MongoDB documents.",
    },
}


def _job_view(job, include_logs=False):
    payload = {
        "id": job["id"],
        "task": job["task"],
        "status": job["status"],
        "created_at": job["created_at"],
        "started_at": job.get("started_at"),
        "completed_at": job.get("completed_at"),
        "returncode": job.get("returncode"),
        "pid": job.get("pid"),
        "command": job["command"],
        "output_file": job.get("output_file"),
        "output_exists": _job_output_exists(job),
        "duration_seconds": job.get("duration_seconds"),
        "payload": job["payload"],
        "error": job.get("error"),
        "log_line_count": job.get("log_line_count", 0),
        "log_truncated": job.get("log_truncated", False),
    }
    if include_logs:
        payload["logs"] = list(job.get("logs", []))
    return payload


def _job_state_path(job_id):
    return JOBS_DIR / f"{job_id}.json"


def _job_output_exists(job):
    output_file = str(job.get("output_file") or "").strip()
    return bool(output_file and Path(output_file).exists())


def _job_store_enabled():
    return bool(JOB_MONGO_URI)


def _warn_job_store_issue(message):
    global JOB_STORE_WARNED
    if JOB_STORE_WARNED:
        return
    JOB_STORE_WARNED = True
    print(f"[job-store] {message}", flush=True)


def _job_collection():
    if not _job_store_enabled():
        return None, None

    client = MongoClient(
        JOB_MONGO_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=5000,
    )
    return client, client[JOB_MONGO_DB][JOB_MONGO_COLLECTION]


def _snapshot_job(job):
    snapshot = dict(job)
    snapshot["command"] = list(job.get("command", []))
    payload = job.get("payload", {})
    snapshot["payload"] = dict(payload) if isinstance(payload, dict) else payload
    snapshot["logs"] = list(job.get("logs", []))
    return snapshot


def _persist_job_snapshot(snapshot):
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    target_path = _job_state_path(snapshot["id"])
    temp_path = target_path.with_suffix(".json.tmp")
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, ensure_ascii=False, indent=2)
    temp_path.replace(target_path)

    if not _job_store_enabled():
        return

    client = None
    try:
        client, collection = _job_collection()
        if collection is not None:
            collection.replace_one({"id": snapshot["id"]}, snapshot, upsert=True)
    except Exception as exc:
        _warn_job_store_issue(f"Mongo persist failed: {exc}")
    finally:
        if client is not None:
            client.close()


def _persist_job(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        snapshot = _snapshot_job(job)
    _persist_job_snapshot(snapshot)


def _recover_job_state(job):
    recovered = _snapshot_job(job)
    if recovered.get("status") in {"queued", "running"}:
        recovered["status"] = "failed"
        recovered["completed_at"] = recovered.get("completed_at") or _now_iso()
        recovered["error"] = (
            recovered.get("error")
            or "Job was interrupted by a service restart before completion."
        )
        recovered["returncode"] = None
        recovered.pop("started_monotonic", None)
    return recovered


def _load_job_from_disk(job_id):
    state_path = _job_state_path(job_id)
    if not state_path.exists():
        return None

    try:
        with open(state_path, "r", encoding="utf-8") as handle:
            job = json.load(handle)
    except Exception:
        return None

    recovered = _recover_job_state(job)
    if recovered != job:
        _persist_job_snapshot(recovered)
    return recovered


def _load_job_from_mongo(job_id):
    if not _job_store_enabled():
        return None

    client = None
    try:
        client, collection = _job_collection()
        if collection is None:
            return None

        job = collection.find_one({"id": job_id}, {"_id": 0})
        if not job:
            return None

        recovered = _recover_job_state(job)
        if recovered != job:
            collection.replace_one({"id": recovered["id"]}, recovered, upsert=True)
        return recovered
    except Exception as exc:
        _warn_job_store_issue(f"Mongo load failed for job {job_id}: {exc}")
        return None
    finally:
        if client is not None:
            client.close()


def _load_recent_jobs_from_mongo(limit=50):
    if not _job_store_enabled():
        return

    client = None
    try:
        client, collection = _job_collection()
        if collection is None:
            return

        cursor = collection.find(
            {},
            {"_id": 0},
        ).sort("created_at", -1).limit(max(1, int(limit)))

        for job in cursor:
            recovered = _recover_job_state(job)
            job_id = recovered.get("id")
            if not job_id:
                continue

            with JOBS_LOCK:
                if job_id not in JOBS:
                    JOBS[job_id] = recovered

            if recovered != job:
                collection.replace_one({"id": recovered["id"]}, recovered, upsert=True)
    except Exception as exc:
        _warn_job_store_issue(f"Mongo recent-jobs load failed: {exc}")
        return
    finally:
        if client is not None:
            client.close()


def _load_jobs_from_disk():
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    for state_path in JOBS_DIR.glob("*.json"):
        try:
            with open(state_path, "r", encoding="utf-8") as handle:
                job = json.load(handle)
        except Exception:
            continue

        recovered = _recover_job_state(job)
        job_id = recovered.get("id")
        if not job_id:
            continue

        with JOBS_LOCK:
            JOBS[job_id] = recovered

        if recovered != job:
            _persist_job_snapshot(recovered)

    _load_recent_jobs_from_mongo()


def _get_job(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if job:
            return _snapshot_job(job)

    recovered = _load_job_from_disk(job_id)
    if not recovered:
        recovered = _load_job_from_mongo(job_id)
    if not recovered:
        return None

    with JOBS_LOCK:
        JOBS[job_id] = recovered
    return _snapshot_job(recovered)


def _read_job_output(job):
    output_file = str(job.get("output_file") or "").strip()
    if not output_file:
        raise FileNotFoundError("This job does not have an output file.")

    output_path = Path(output_file)
    if not output_path.exists():
        raise FileNotFoundError(f"Output file not found: {output_file}")

    raw_text = output_path.read_text(encoding="utf-8")
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        return {"raw": raw_text}


_load_jobs_from_disk()


def _append_job_log(job_id, line):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        logs = job.setdefault("logs", [])
        logs.append(line.rstrip())
        job["log_line_count"] = job.get("log_line_count", 0) + 1
        if len(logs) > MAX_LOG_LINES:
            del logs[0 : len(logs) - MAX_LOG_LINES]
            job["log_truncated"] = True
    _persist_job(job_id)


def _summarize_jobs():
    summary = {"queued": 0, "running": 0, "completed": 0, "failed": 0}
    with JOBS_LOCK:
        for job in JOBS.values():
            status = job.get("status", "queued")
            summary[status] = summary.get(status, 0) + 1
    summary["max_concurrent_jobs"] = MAX_CONCURRENT_JOBS
    return summary


def _run_job(job_id):
    acquired_slot = False
    try:
        JOB_SEMAPHORE.acquire()
        acquired_slot = True

        with JOBS_LOCK:
            job = JOBS[job_id]
            job["status"] = "running"
            job["started_at"] = _now_iso()
            job["started_monotonic"] = time.monotonic()
            command = list(job["command"])
        _persist_job(job_id)

        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        try:
            process = subprocess.Popen(
                command,
                cwd=str(BASE_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env=env,
            )
        except Exception as exc:
            with JOBS_LOCK:
                job = JOBS[job_id]
                started_monotonic = job.get("started_monotonic")
                job["status"] = "failed"
                job["completed_at"] = _now_iso()
                job["duration_seconds"] = round(
                    time.monotonic() - started_monotonic,
                    3,
                ) if started_monotonic else 0
                job["error"] = str(exc)
                job["returncode"] = None
                job.pop("started_monotonic", None)
            _persist_job(job_id)
            return

        with JOBS_LOCK:
            JOBS[job_id]["pid"] = process.pid
        _persist_job(job_id)

        if process.stdout is not None:
            for raw_line in process.stdout:
                _append_job_log(job_id, raw_line)

        returncode = process.wait()

        with JOBS_LOCK:
            job = JOBS[job_id]
            started_monotonic = job.get("started_monotonic")
            job["completed_at"] = _now_iso()
            job["duration_seconds"] = round(
                time.monotonic() - started_monotonic,
                3,
            ) if started_monotonic else None
            job["returncode"] = returncode
            job["status"] = "completed" if returncode == 0 else "failed"
            job.pop("started_monotonic", None)
        _persist_job(job_id)
    except Exception as exc:
        with JOBS_LOCK:
            job = JOBS[job_id]
            started_monotonic = job.get("started_monotonic")
            job["status"] = "failed"
            job["completed_at"] = _now_iso()
            job["duration_seconds"] = round(
                time.monotonic() - started_monotonic,
                3,
            ) if started_monotonic else None
            job["error"] = str(exc)
            job["returncode"] = None
            job.pop("started_monotonic", None)
        _persist_job(job_id)
    finally:
        if acquired_slot:
            JOB_SEMAPHORE.release()


def _start_job(task_name, payload):
    normalized_task = _normalize_task_name(task_name)
    spec = TASKS.get(normalized_task)
    if not spec:
        raise ValueError(f"Unsupported task: {task_name}")

    payload = _ensure_dict_payload(payload)
    output_file = _build_output_path(normalized_task) if spec["uses_output_file"] else None
    command = spec["builder"](payload, output_file)
    job_id = uuid.uuid4().hex

    job = {
        "id": job_id,
        "task": normalized_task,
        "status": "queued",
        "created_at": _now_iso(),
        "payload": payload,
        "command": command,
        "output_file": output_file,
        "logs": [],
        "log_line_count": 0,
        "log_truncated": False,
        "returncode": None,
        "error": None,
        "pid": None,
    }

    with JOBS_LOCK:
        JOBS[job_id] = job
    _persist_job(job_id)

    worker = threading.Thread(target=_run_job, args=(job_id,), daemon=True)
    worker.start()
    return _job_view(job)


def _read_json_body(handler):
    length = int(handler.headers.get("Content-Length", "0"))
    if length <= 0:
        return {}
    raw_body = handler.rfile.read(length).decode("utf-8")
    if not raw_body.strip():
        return {}
    return json.loads(raw_body)


class ScraperRequestHandler(BaseHTTPRequestHandler):
    server_version = "ScraperAPI/1.0"

    def log_message(self, format_, *args):
        return

    def _send_json(self, status_code, payload):
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self._send_json(200, {"ok": True})

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"

        if path == "/":
            self._send_json(
                200,
                {
                    "service": "scraper-api",
                    "python": PYTHON_EXECUTABLE,
                    "routes": {
                        "GET /health": "Basic health check",
                        "GET /tasks": "Available tasks",
                        "GET /jobs": "List jobs",
                        "GET /jobs/<job_id>": "Get one job with recent logs",
                        "GET /jobs/<job_id>/output": "Read one job's output JSON",
                        "POST /run/<task>": "Start one task",
                        "POST /run-all": "Start multiple tasks together",
                    },
                },
            )
            return

        if path == "/health":
            self._send_json(
                200,
                {
                    "status": "ok",
                    "python": PYTHON_EXECUTABLE,
                    "base_dir": str(BASE_DIR),
                    "jobs": _summarize_jobs(),
                },
            )
            return

        if path == "/tasks":
            tasks = {
                task_name: {
                    "requires_url": spec["requires_url"],
                    "uses_output_file": spec["uses_output_file"],
                    "description": spec["description"],
                }
                for task_name, spec in TASKS.items()
            }
            self._send_json(200, {"tasks": tasks})
            return

        if path == "/jobs":
            with JOBS_LOCK:
                jobs = sorted(
                    (_job_view(job) for job in JOBS.values()),
                    key=lambda item: item["created_at"],
                    reverse=True,
                )
            self._send_json(200, {"jobs": jobs})
            return

        path_parts = [part for part in path.split("/") if part]

        if len(path_parts) == 3 and path_parts[0] == "jobs" and path_parts[2] == "output":
            job_id = path_parts[1]
            job = _get_job(job_id)
            if not job:
                self._send_json(404, {"error": "Job not found."})
                return

            try:
                output_payload = _read_job_output(job)
            except FileNotFoundError as exc:
                status_code = 202 if job.get("status") in {"queued", "running"} else 404
                self._send_json(
                    status_code,
                    {
                        "error": str(exc),
                        "job": _job_view(job),
                    },
                )
                return

            self._send_json(
                200,
                {
                    "job": _job_view(job),
                    "output": output_payload,
                },
            )
            return

        if len(path_parts) == 2 and path_parts[0] == "jobs":
            job_id = path_parts[1]
            job = _get_job(job_id)
            if not job:
                self._send_json(404, {"error": "Job not found."})
                return
            self._send_json(200, _job_view(job, include_logs=True))
            return

        self._send_json(404, {"error": "Route not found."})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"

        try:
            payload = _read_json_body(self)
        except json.JSONDecodeError as exc:
            self._send_json(400, {"error": f"Invalid JSON: {exc.msg}"})
            return

        try:
            payload = _ensure_dict_payload(payload)

            if path.startswith("/run/"):
                raw_task = path.split("/", 2)[2]
                job = _start_job(raw_task, payload)
                self._send_json(202, {"job": job})
                return

            if path == "/run-all":
                job_payloads = payload.get("jobs", payload)
                if not isinstance(job_payloads, dict) or not job_payloads:
                    raise ValueError("`jobs` must be a non-empty object.")

                normalized = []
                for raw_task, task_payload in job_payloads.items():
                    normalized.append((raw_task, _ensure_dict_payload(task_payload)))

                jobs = [_start_job(raw_task, task_payload) for raw_task, task_payload in normalized]
                self._send_json(202, {"jobs": jobs})
                return
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
            return
        except Exception as exc:
            self._send_json(500, {"error": str(exc)})
            return

        self._send_json(404, {"error": "Route not found."})


def main():
    server = ThreadingHTTPServer((HOST, PORT), ScraperRequestHandler)
    print(f"Scraper API listening on http://{HOST}:{PORT}")
    print(f"Using Python interpreter: {PYTHON_EXECUTABLE}")
    print("Available tasks:", ", ".join(sorted(TASKS.keys())))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down scraper API...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
