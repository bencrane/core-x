"""Universal Dispatcher — the single proxy-authed Modal entrypoint for the fleet.

ONE endpoint for every feed. Trigger.dev POSTs
``{app_name, function_name, kwargs, trigger_callback_url}``; the dispatcher
resolves the target Modal function by name, ``spawn()``s it (fire-and-forget,
the HTTP connection is never held open for the job), and returns 202 Accepted.
The spawned worker runs in the background and POSTs its terminal metadata to
``trigger_callback_url`` to wake the suspended Trigger run.

This is what kills env-var bloat: exactly ONE exposed endpoint
(``MODAL_DISPATCHER_URL``) and ONE proxy-auth pair (``MODAL_KEY`` /
``MODAL_SECRET``) for the whole fleet, forever. A new feed adds a Modal worker
and a one-line Trigger task — zero new endpoints, zero new secrets.

    modal deploy core/modal_dispatcher.py
"""

from __future__ import annotations

import hmac
import os

import modal
from fastapi import Request
from pydantic import BaseModel

app = modal.App("universal-dispatcher")

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "fastapi[standard]>=0.115",
    "pydantic>=2",
)


class DispatchRequest(BaseModel):
    app_name: str
    function_name: str
    kwargs: dict = {}
    trigger_callback_url: str


def _verify_auth(request: Request) -> bool:
    expected_key = os.environ.get("MODAL_KEY", "")
    expected_secret = os.environ.get("MODAL_SECRET", "")
    if not expected_key or not expected_secret:
        return False

    key = (
        request.headers.get("modal-key")
        or request.headers.get("x-modal-key")
        or request.query_params.get("modal_key")
        or request.query_params.get("key")
        or ""
    )
    secret = (
        request.headers.get("modal-secret")
        or request.headers.get("x-modal-secret")
        or request.query_params.get("modal_secret")
        or request.query_params.get("secret")
        or ""
    )

    if not key and not secret:
        auth = request.headers.get("authorization") or ""
        if auth.lower().startswith("bearer "):
            token = auth[7:].strip()
            if "." in token:
                key, secret = token.split(".", 1)

    return hmac.compare_digest(key, expected_key) and hmac.compare_digest(
        secret, expected_secret
    )


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("dispatcher-auth")],
)
@modal.fastapi_endpoint(method="POST", label="dispatch")
def dispatch(req: DispatchRequest, request: Request):
    """Spawn the named worker and return immediately.

    NOTE on the spawn signature: the directive writes
    ``.spawn(kwargs, trigger_callback_url=...)``, but passing the dict
    positionally would bind it to the worker's first parameter and collide
    with the explicit ``trigger_callback_url`` keyword. The correct form
    spreads kwargs as keyword args — the worker receives its feed-specific
    parameters plus ``trigger_callback_url``.
    """
    from fastapi.responses import JSONResponse

    if not _verify_auth(request):
        return JSONResponse(
            status_code=401,
            content={"error": "invalid credentials for dispatcher authorization"},
        )

    fn = modal.Function.from_name(req.app_name, req.function_name)
    call = fn.spawn(**req.kwargs, trigger_callback_url=req.trigger_callback_url)

    # 202 Accepted — work acknowledged, result delivered later via the callback.
    return JSONResponse(
        status_code=202,
        content={
            "accepted": True,
            "call_id": call.object_id,
            "app": req.app_name,
            "function": req.function_name,
        },
    )
