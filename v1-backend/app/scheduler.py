from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from datetime import timedelta
from typing import Callable
import threading
import traceback

_sched = None


def parse_simple_schedule(spec: str):
    s = (spec or "").strip()
    if not s:
        return None
    if s.endswith("m") and s[:-1].isdigit():
        minutes = int(s[:-1])
        return IntervalTrigger(minutes=minutes)
    if s.endswith("h") and s[:-1].isdigit():
        hours = int(s[:-1])
        return IntervalTrigger(hours=hours)
    if s == "daily":
        return IntervalTrigger(days=1)
    # try cron expression
    parts = s.split()
    if len(parts) >= 5:
        try:
            return CronTrigger.from_crontab(s)
        except Exception:
            return None
    return None


def _run_job_safe(fn: Callable, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except Exception:
        traceback.print_exc()


def start_scheduler():
    global _sched
    if _sched is not None:
        return _sched
    _sched = BackgroundScheduler()
    _sched.start()
    return _sched


def stop_scheduler():
    global _sched
    if _sched:
        _sched.shutdown(wait=False)
        _sched = None


def schedule_job(job_id: str, spec: str, func: Callable, args=None, kwargs=None):
    """Schedule a job by id. spec is simple like '5m','15m','1h','daily' or cron."""
    trig = parse_simple_schedule(spec)
    if trig is None:
        return False
    args = args or []
    kwargs = kwargs or {}
    start_scheduler()
    # remove existing job if present
    try:
        _sched.remove_job(job_id)
    except Exception:
        pass
    _sched.add_job(_run_job_safe, trigger=trig, id=job_id, args=[func] + args, kwargs=kwargs)
    return True


def remove_scheduled(job_id: str):
    if _sched is None:
        return
    try:
        _sched.remove_job(job_id)
    except Exception:
        pass
