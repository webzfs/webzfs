import logging
import threading

from fastapi import FastAPI, Request, Response
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from auth.exceptions import AuthenticationFailed
from config.security_headers import SecurityHeadersMiddleware
from config.settings import settings
from views import router

logger = logging.getLogger(__name__)


def reconcile_scheduled_tasks() -> None:
    """Converge the OS scheduler with the stored schedules.

    Runs once at startup so systemd timers or the root crontab match
    what is in the schedule stores. This matters after an update (units
    may be missing) and for legacy records that were stored before the
    Unified Scheduling Hub actually registered them with the OS.

    Failures are logged and ignored: a scheduler problem must never
    prevent the web interface from starting.
    """
    try:
        from services.job_scheduler import TaskScheduler
        TaskScheduler().sync_all()
        logger.info("Scheduled tasks reconciled with the OS scheduler")
    except Exception as sync_error:
        logger.warning(f"Could not reconcile scheduled tasks: {sync_error}")


def create_app() -> FastAPI:
    app = FastAPI(debug=settings.DEBUG)
    app.add_middleware(SecurityHeadersMiddleware)
    app.mount("/static", StaticFiles(directory="static"), name="static")
    app.include_router(router)

    @app.exception_handler(AuthenticationFailed)
    def redirect_to_login(request: Request, exc: AuthenticationFailed) -> Response:
        return RedirectResponse("/login/")

    @app.on_event("startup")
    def report_login_policy() -> None:
        from auth.login_policy import log_policy_at_startup
        log_policy_at_startup()

    @app.on_event("startup")
    def start_scheduler_reconciliation() -> None:
        # Done in a background thread because writing unit files calls
        # sudo and can take a second or two per task.
        threading.Thread(
            target=reconcile_scheduled_tasks,
            name="scheduler-reconcile",
            daemon=True,
        ).start()

    return app
