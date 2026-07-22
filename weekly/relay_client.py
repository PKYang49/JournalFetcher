"""Shared authentication headers for the Cloudflare Access relay."""

from __future__ import annotations

import os


def access_headers() -> dict[str, str]:
    """Return service-token headers, or no headers for the legacy relay.

    Both values must be set together so a partial Cloudflare configuration
    fails clearly instead of producing a misleading Access login response.
    """
    client_id = os.getenv("CF_ACCESS_CLIENT_ID", "").strip()
    client_secret = os.getenv("CF_ACCESS_CLIENT_SECRET", "").strip()
    if not client_id and not client_secret:
        return {}
    if not client_id or not client_secret:
        raise RuntimeError(
            "CF_ACCESS_CLIENT_ID and CF_ACCESS_CLIENT_SECRET must both be set"
        )
    return {
        "CF-Access-Client-Id": client_id,
        "CF-Access-Client-Secret": client_secret,
    }
