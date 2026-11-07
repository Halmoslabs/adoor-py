"""Get a credential without pasting one: approve a notebook in the browser, or exchange a workload's OIDC token."""

from __future__ import annotations

import json
import os
import re
import stat
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .client import DEFAULT_URL, VERSION, Client, _NoRedirect, _origin
from .errors import AdoorError, error_from_body

__all__ = ["login", "exchange", "credentials_path"]

_USER_CODE = re.compile(r"^[BCDFGHJKLMNPQRSTVWXZ23456789]{4}-[BCDFGHJKLMNPQRSTVWXZ23456789]{4}$")
_TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"
_JWT = "urn:ietf:params:oauth:token-type:jwt"


def _post(url: str, path: str, body: bytes, content_type: str, timeout: float) -> Dict[str, Any]:
    request = urllib.request.Request(url + path, data=body, method="POST", headers={
        "Content-Type": content_type, "Accept": "application/json", "User-Agent": f"adoor-python/{VERSION}"})
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        with opener.open(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        with error:
            body = error.read()
        raise error_from_body(error.code, body) from None
    except (urllib.error.URLError, OSError) as error:
        raise AdoorError("NETWORK", f"The request could not reach {url}: {getattr(error, 'reason', error)}", 0) from error


def _same_server(link: str, origin: str) -> bool:
    left, right = urllib.parse.urlsplit(link), urllib.parse.urlsplit(origin)
    return (left.scheme, left.netloc) == (right.scheme, right.netloc)


def credentials_path() -> Path:
    """Where ``login`` keeps credentials: ``$ADOOR_CONFIG_DIR`` or ``~/.config/adoor``, file mode 0600."""
    base = os.environ.get("ADOOR_CONFIG_DIR") or os.path.join(os.environ.get("XDG_CONFIG_HOME") or
                                                              os.path.join(Path.home(), ".config"), "adoor")
    return Path(base) / "credentials.json"


def _read_saved() -> Dict[str, Any]:
    path = credentials_path()
    try:
        return json.loads(path.read_text("utf-8"))
    except (OSError, ValueError):
        return {}


def _save(key: str, token: str) -> None:
    path = credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    saved = _read_saved()
    saved[key] = {"token": token, "savedAt": int(time.time())}
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, stat.S_IRUSR | stat.S_IWUSR)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(saved, handle, indent=2)
    os.replace(temporary, path)


def _forget(key: str) -> None:
    saved = _read_saved()
    if key in saved:
        del saved[key]
        path = credentials_path()
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, stat.S_IRUSR | stat.S_IWUSR)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(saved, handle, indent=2)


def _announce(approval_url: str, user_code: str) -> None:
    text = f"Approve this connection in ADoor: {approval_url}\nCheck that the page shows the code {user_code}."
    try:
        from IPython.display import HTML, display  # type: ignore[import-not-found]

        if "ipykernel" in sys.modules:
            display(HTML(f'<p>Approve this connection in ADoor: <a href="{approval_url}" target="_blank" '
                         f'rel="noopener">{approval_url}</a><br>Check that the page shows the code '
                         f"<code>{user_code}</code>.</p>"))
            return
    except ImportError:
        pass
    print(text, file=sys.stderr)


def login(url: Optional[str] = None, *, name: str = "Python", save: bool = True, open_browser: bool = True,
          timeout: float = 600.0, **options: Any) -> Client:
    """Sign in from a script or notebook: ADoor opens in the browser and you choose what this connection can reach.

    The connection is named ``name`` (for example ``"Growth curves notebook"``), and every change it makes
    shows that name in Activity. With ``save=True`` the credential is kept in ``credentials_path()`` and
    reused next time, so rerunning the cell does not ask again.
    """
    origin = _origin(url or os.environ.get("ADOOR_URL") or DEFAULT_URL)
    key = f"{origin} {name}"
    if save:
        saved = _read_saved().get(key)
        if isinstance(saved, dict) and isinstance(saved.get("token"), str):
            client = Client(origin, saved["token"], **options)
            try:
                client.whoami()
                return client
            except AdoorError as error:
                if not error.authority:
                    raise
                _forget(key)
    started = _post(origin, "/api/auth/device", json.dumps({"name": name}).encode("utf-8"), "application/json", 30.0)
    device_code, user_code, approval_url = started.get("deviceCode"), started.get("userCode"), started.get("approvalUrl")
    if not isinstance(device_code, str) or not isinstance(user_code, str) or not _USER_CODE.match(user_code) \
            or not isinstance(approval_url, str) or not _same_server(approval_url, origin):
        raise AdoorError("INVALID_RESPONSE", "The server returned an invalid sign-in request.", 502)
    _announce(approval_url, user_code)
    if open_browser:
        try:
            webbrowser.open(approval_url)
        except webbrowser.Error:
            pass
    interval = max(1.0, float(started.get("interval") or 2))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        polled = _post(origin, "/api/auth/device/poll", json.dumps({"deviceCode": device_code}).encode("utf-8"),
                       "application/json", 30.0)
        status = polled.get("status")
        if status == "approved":
            token = polled.get("token")
            if not isinstance(token, str) or not token:
                raise AdoorError("INVALID_RESPONSE", "Sign-in completed without a credential.", 502)
            client = Client(origin, token, **options)
            client.whoami()
            if save:
                _save(key, token)
            return client
        if status in ("expired", "consumed", "denied"):
            raise AdoorError("LOGIN_EXPIRED", f"This sign-in was {status}. Call adoor.login() to start again.", 401)
        time.sleep(min(10.0, interval))
    raise AdoorError("LOGIN_EXPIRED", "Sign-in was not approved in time. Call adoor.login() to start again.", 401)


def exchange(subject_token: str, trust_id: str, url: Optional[str] = None,
             **options: Any) -> Client:
    """Exchange a workload's OIDC token (GitHub Actions, a cloud sandbox) for a short-lived ADoor identity.

    ``trust_id`` is the trust a person approved in ADoor for that issuer and subject. The child it returns
    is capped by that trust and expires within 15 minutes; call again for a fresh one.
    """
    origin = _origin(url or os.environ.get("ADOOR_URL") or DEFAULT_URL)
    form = urllib.parse.urlencode({"grant_type": _TOKEN_EXCHANGE, "subject_token": subject_token,
                                   "subject_token_type": _JWT, "audience": trust_id}).encode("ascii")
    response = _post(origin, "/oauth/token", form, "application/x-www-form-urlencoded", 30.0)
    token = response.get("access_token")
    if not isinstance(token, str) or not token:
        raise AdoorError("INVALID_RESPONSE", "The token exchange returned no access token.", 502)
    return Client(origin, token, **options)


def github_actions_token(audience: str, *, environ: Optional[Dict[str, str]] = None,
                         fetch: Optional[Callable[[str, str], str]] = None) -> str:
    """The GitHub Actions OIDC token for ``audience`` (the ADoor origin). The job needs ``id-token: write``."""
    env = environ if environ is not None else dict(os.environ)
    request_url, request_token = env.get("ACTIONS_ID_TOKEN_REQUEST_URL"), env.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    if not request_url or not request_token:
        raise AdoorError("NO_WORKLOAD_TOKEN", "This job has no OIDC token. Add `permissions: id-token: write`.", 400)
    separator = "&" if "?" in request_url else "?"
    target = f"{request_url}{separator}audience={urllib.parse.quote(audience, safe='')}"
    if fetch is not None:
        return fetch(target, request_token)
    request = urllib.request.Request(target, headers={"Authorization": f"Bearer {request_token}"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))["value"]
