"""Checkpoint management for idempotent execution processing.

This module provides simple, file-based checkpointing to track the last
successfully processed n8n execution ID. This allows the shipper process to
be stopped and resumed without reprocessing data.

The store_checkpoint function uses an atomic write pattern (write to a
temporary file then rename) to prevent checkpoint corruption if the process is
interrupted.
"""
from __future__ import annotations

import os
from typing import Optional


def load_checkpoint(path: str) -> tuple[Optional[str], Optional[int]]:
    """Load the last processed cursor (stoppedAt, id) from a checkpoint file.

    Returns:
        A tuple (last_stopped_at_iso_string, last_id).
        Returns (None, None) if file invalid or missing.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        if not raw:
            return None, None
        
        # Format: "iso_timestamp|id" or just "id" (legacy)
        parts = raw.split("|", 1)
        if len(parts) == 2:
            return parts[0], int(parts[1])
        # Legacy fallback: just ID, no timestamp
        return None, int(raw)
    except FileNotFoundError:
        return None, None
    except Exception:
        return None, None


def store_checkpoint(path: str, execution_id: int, stopped_at_iso: Optional[str] = None) -> None:
    """Atomically store cursor to the checkpoint file.
    
    Format: "YYYY-MM-DDTHH:MM:SS.mmmmmm+HH:MM|ID"
    """
    tmp_path = f"{path}.tmp"
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(tmp_path, "w", encoding="utf-8") as f:
        if stopped_at_iso is not None:
            f.write(f"{stopped_at_iso}|{execution_id}")
        else:
            f.write(str(execution_id))
    os.replace(tmp_path, path)


__all__ = ["load_checkpoint", "store_checkpoint"]
