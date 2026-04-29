#!/usr/bin/env python3
"""Run the silent export worker once (for demo/testing)."""
from app.silent_export_worker import process_once


if __name__ == "__main__":
    process_once()
