from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass
class AmsAuthConfig:
    auth: Optional[Tuple[str, str]]
    headers: Dict[str, str]


def build_ams_auth() -> AmsAuthConfig:
    auth_type = os.environ.get("AMS_AUTH_TYPE", "").strip().lower()
    api_key = os.environ.get("AMS_API_KEY", "").strip()

    if not auth_type:
        raise RuntimeError(
            "AMS auth is not configured. Set AMS_AUTH_TYPE to 'basic' or 'header'."
        )
    if not api_key:
        raise RuntimeError("AMS auth is not configured. Set AMS_API_KEY.")

    if auth_type == "basic":
        # MARS API commonly uses key as username with empty password.
        return AmsAuthConfig(auth=(api_key, ""), headers={})

    if auth_type == "header":
        header_name = os.environ.get("AMS_HEADER_NAME", "").strip()
        if not header_name:
            raise RuntimeError(
                "AMS_AUTH_TYPE=header requires AMS_HEADER_NAME to be set."
            )
        return AmsAuthConfig(auth=None, headers={header_name: api_key})

    raise RuntimeError(
        f"Invalid AMS_AUTH_TYPE='{auth_type}'. Use 'basic' or 'header'."
    )

