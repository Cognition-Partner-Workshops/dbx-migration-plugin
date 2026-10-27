"""Oracle DSN secret parsing shared by the recon harness and factory-doctor; errors never echo values."""

from __future__ import annotations

import json
import re


# oracle://user:pass@host:port/service and user/pass@host:port/service; the form some managers
# emit (host:port:SID or a TNS alias) is refused rather than guessed at.
_ORACLE_DSN_RE = re.compile(
    r"^(?:oracle://)?(?P<user>[^:/@\s]+)[:/](?P<password>[^@\s]*)@(?P<host>[^:/@\s]+)"
    r":(?P<port>\d+)/(?P<service>[^\s/]+)\s*$")

_FORMS = ("oracle://user:pass@host:port/service, user/pass@host:port/service, or a JSON object "
          "{user, password, host, port, service}")


def oracle_dsn_parts(raw: str) -> tuple[str, str, str]:
    if raw.lstrip().startswith("{"):
        try:
            obj = json.loads(raw)
        except ValueError:
            raise ValueError(f"oracle DSN is not valid JSON; use {_FORMS}") from None
        if not isinstance(obj, dict):
            raise ValueError(f"oracle DSN JSON must be an object; use {_FORMS}")
        user = obj.get("user", obj.get("username"))
        password = obj.get("password")
        host = obj.get("host")
        port = obj.get("port")
        service = obj.get("service", obj.get("service_name"))
        missing = [key for key, value in (("user", user), ("password", password), ("host", host),
            ("port", port), ("service", service)) if value is None]
        if missing:
            raise ValueError(f"oracle DSN JSON is missing {', '.join(missing)}; use {_FORMS}")
        if not isinstance(user, str) or not user:
            raise ValueError("oracle DSN JSON key user must be a non-empty string")
        if not isinstance(password, str):
            raise ValueError("oracle DSN JSON key password must be a string")
        if not isinstance(host, str) or not host:
            raise ValueError("oracle DSN JSON key host must be a non-empty string")
        if not isinstance(service, str) or not service:
            raise ValueError("oracle DSN JSON key service must be a non-empty string")
        if isinstance(port, bool) or not isinstance(port, int) and not (
                isinstance(port, str) and port.isdigit()):
            raise ValueError("oracle DSN JSON key port must be an integer or a string of digits")
        if isinstance(port, str):
            try:
                port = int(port)
            except ValueError:
                raise ValueError(
                    "oracle DSN JSON key port must be an integer or a string of digits") from None
        if any(char in host for char in ":/@") or any(char.isspace() for char in host):
            raise ValueError("oracle DSN JSON key host contains invalid characters")
        if "/" in service or any(char.isspace() for char in service):
            raise ValueError("oracle DSN JSON key service contains invalid characters")
        return user, password, f"{host}:{int(port)}/{service}"

    m = _ORACLE_DSN_RE.match(raw)
    if not m:
        raise ValueError(f"oracle DSN must be {_FORMS}")
    return m["user"], m["password"], f"{m['host']}:{m['port']}/{m['service']}"
