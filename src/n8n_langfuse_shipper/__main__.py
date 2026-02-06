"""Main CLI entry point for the n8n-langfuse-shipper.

This module provides a command-line interface using Typer to run the shipper
process. It orchestrates the entire ETL pipeline:
1.  Loading configuration and checkpoints.
2.  Streaming execution records from the database (n8n_langfuse_shipper.db).
3.  Parsing and validating the raw data, including handling complex formats like
    pointer-compressed executions.
4.  Mapping the records to Langfuse traces (n8n_langfuse_shipper.mapper).
5.  Exporting the traces via OTLP (n8n_langfuse_shipper.shipper).
6.  Storing the new checkpoint upon successful processing.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import typer
from pydantic import ValidationError

# Load .env file if present (before any config access)
try:
    from dotenv import load_dotenv, find_dotenv

    env_file = find_dotenv(usecwd=True) or find_dotenv()
    if env_file:
        load_dotenv(env_file)
        logging.debug("Loaded environment from %s", env_file)
except Exception:
    pass

from .checkpoint import load_checkpoint, store_checkpoint
from .config import get_settings
from .db import ExecutionSource
from .mapper import map_execution_to_langfuse, map_execution_with_assets
from .media_api import patch_and_upload_media
from .models.n8n import (
    ExecutionData,
    ExecutionDataDetails,
    N8nExecutionRecord,
    ResultData,
    WorkflowData,
)
from .shipper import export_trace, shutdown_exporter

app = typer.Typer(help="n8n to Langfuse shipper shipper CLI")


def _build_execution_data(
    raw_data: Optional[dict[str, Any] | str | list[Any]],
    workflow_data_raw: Optional[dict[str, Any]] = None,
    *,
    debug: bool = False,
    attempt_decompress: bool = False,
    execution_id: Optional[int] = None,
) -> ExecutionData:
    """Robustly parse the `data` column from an n8n execution record.

    The `data` column in `n8n_execution_data` can have several formats. This
    function attempts to find and parse the `runData` object, which contains the
    critical information about each node's execution.

    It employs a resilient, multi-step strategy:
    1.  If the data is a JSON string, it's parsed into a Python object.
    2.  If the data is a list, it's assumed to be the "pointer-compressed"
        format and is decoded by `_decode_compact_pointer_execution`.
    3.  If it's a dictionary, it first attempts a direct Pydantic validation.
    4.  If that fails, it probes a series of common alternative paths where
        `runData` might be nested.
    5.  As a last resort, it checks the raw `workflowData` for `runData`.
    6.  If all attempts fail, it returns an empty `ExecutionData` object,
        ensuring that a root trace is still created for the execution.

    Args:
        raw_data: The raw content of the `data` column.
        workflow_data_raw: The raw `workflowData` object, used as a fallback.
        debug: If True, enables verbose logging of parsing attempts.
        attempt_decompress: Flag to enable future decompression logic.
        execution_id: The ID of the execution, for logging purposes.

    Returns:
        A parsed `ExecutionData` model, which may be empty if `runData` could
        not be found.
    """
    logger = logging.getLogger(__name__)
    empty = ExecutionData(executionData=ExecutionDataDetails(resultData=ResultData(runData={})))

    # Accept JSON string from DB driver if not auto-decoded.
    if isinstance(raw_data, str):
        try:
            import json
            raw_data = json.loads(raw_data)
        except Exception:
            logger.debug("Failed to json.loads execution data string; returning empty runData")
            return empty

    # Optionally attempt decompression if payload looks like base64+gzip and flag enabled.
    if attempt_decompress and isinstance(raw_data, (bytes, str)):
        # Not implemented yet; placeholder for future extension.
        logger.debug("attempt_decompress flag set but decompression logic not implemented; skipping")

    # Pointer-compressed (flatted) format: detect list root and attempt upstream flatted parse.
    # If the DB driver already decoded JSON into a list, re-serialize for parser.
    if isinstance(raw_data, list):
        try:
            import json
            from .vendor.flatted import parse as flatted_parse  # type: ignore
            from .vendor import flatted as _flatted_mod  # for _String unwrap
            def _sanitize_flatted(val: Any) -> Any:
                # Recursively convert leftover _String wrapper instances to raw string values.
                if isinstance(val, getattr(_flatted_mod, "_String")):
                    return val.value
                if isinstance(val, list):
                    return [_sanitize_flatted(x) for x in val]
                if isinstance(val, dict):
                    return {k: _sanitize_flatted(v) for k, v in val.items()}
                return val
            serialized = json.dumps(raw_data)
            parsed_root = flatted_parse(serialized)
            parsed_root = _sanitize_flatted(parsed_root)
            # Expect structure: root.resultData.runData
            result_data_dict = (
                parsed_root.get("resultData", {})
                if isinstance(parsed_root, dict) else {}
            )
            run_data = result_data_dict.get("runData", {})
            meta_data = result_data_dict.get("metadata")

            if isinstance(run_data, dict) and run_data:
                if debug:
                    logger.info(
                        "Execution %s: Parsed flatted pointer-compressed format with %d node keys",
                        execution_id,
                        len(run_data),
                    )
                return ExecutionData(
                    executionData=ExecutionDataDetails(
                        resultData=ResultData(runData=run_data, metadata=meta_data)
                    )
                )
        except Exception as e:  # pragma: no cover - fail open
            if debug:
                logger.warning(
                    "Execution %s: flatted parse failed (%s); falling back to other paths",
                    execution_id,
                    e,
                )

    if not raw_data or not isinstance(raw_data, dict):
        # Fallback: attempt to derive runData from workflowData raw if provided (edge cases / custom storage)
        if workflow_data_raw and isinstance(workflow_data_raw, dict):
            maybe_rd = workflow_data_raw.get("runData") or workflow_data_raw.get("resultData", {}).get("runData")
            if isinstance(maybe_rd, dict) and maybe_rd:
                logger.warning("Using workflowData payload as source for runData (data column empty)")
                return ExecutionData(
                    executionData=ExecutionDataDetails(resultData=ResultData(runData=maybe_rd))
                )
        return empty

    # Helper to materialize ExecutionData from run_data and optional metadata
    def _from_result_data(rd: dict[str, Any], md: Optional[dict[str, Any]] = None) -> ExecutionData:
        return ExecutionData(
            executionData=ExecutionDataDetails(
                resultData=ResultData(runData=rd, metadata=md)
            )
        )

    # Attempt full pydantic parse first if key present
    if "executionData" in raw_data:
        try:
            parsed = ExecutionData(**raw_data)
            if parsed.executionData.resultData.runData:
                if debug:
                    logger.info(
                        "Execution %s: Parsed runData via standard path with %d node keys",
                        execution_id,
                        len(parsed.executionData.resultData.runData),
                    )
                return parsed
            elif debug:
                logger.info("Execution %s: executionData present but runData empty", execution_id)
        except ValidationError as ve:
            logger.debug("ExecutionData validation failed: %s", ve)

    # Probe multiple candidate paths for ResultData (containing runData)
    # Each candidate is a dict that MIGHT contain runData and metadata
    result_data_candidates: list[Any] = []

    # Path 1: raw_data.executionData.resultData
    try:
        result_data_candidates.append(raw_data.get("executionData", {}).get("resultData"))
    except Exception: pass

    # Path 2: raw_data.resultData
    try:
        result_data_candidates.append(raw_data.get("resultData"))
    except Exception: pass

    # Path 3: raw_data itself (if it has runData directly)
    result_data_candidates.append(raw_data)

    # Path 4: nested data key
    try:
        nested = raw_data.get("data")
        if isinstance(nested, dict):
             result_data_candidates.append(nested.get("executionData", {}).get("resultData"))
             result_data_candidates.append(nested.get("resultData"))
    except Exception: pass

    for cand in result_data_candidates:
        if isinstance(cand, dict) and cand.get("runData"):
             rd = cand["runData"]
             md = cand.get("metadata")
             if isinstance(rd, dict) and rd:
                 if debug:
                    logger.info(
                        "Execution %s: Recovered runData via alternative path with %d node keys",
                        execution_id,
                        len(rd),
                    )
                 return _from_result_data(rd, md)

    # Last chance: workflowData fallback (non-standard)
    if workflow_data_raw and isinstance(workflow_data_raw, dict):
        maybe_rd = workflow_data_raw.get("runData") or workflow_data_raw.get("resultData", {}).get("runData")
        if isinstance(maybe_rd, dict) and maybe_rd:
            logger.warning("Recovered runData from workflowData (non-standard storage)")
            return _from_result_data(maybe_rd)

    if debug:
        logger.warning(
            "Execution %s: runData empty (data.keys=%s workflowData.keys=%s)",
            execution_id,
            list(raw_data.keys())[:20],
            list(workflow_data_raw.keys())[:20] if isinstance(workflow_data_raw, dict) else None,
        )
    return empty


@app.callback()
def main() -> None:  # pragma: no cover - simple callback
    """n8n-langfuse-shipper CLI.

    Use a subcommand like 'shipper' to run a process.
    """
    pass


@app.command(help="Run a single shipper cycle (Iteration 2 basic mapping).")
def shipper(
    start_after_id: Optional[int] = typer.Option(
        None, help="Start processing executions with id greater than this value (secondary cursor). Use checkpoint file for full state."
    ),
    limit: Optional[int] = typer.Option(
        None, help="Maximum number of executions to process in this run"
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run/--no-dry-run",
        help="If true, do not send spans to Langfuse (mapping only). If not specified, uses DRY_RUN from config/env.",
    ),
    checkpoint_file: Optional[str] = typer.Option(
        None, help="Path to checkpoint file (defaults to settings.CHECKPOINT_FILE)"
    ),
    debug: bool = typer.Option(
        False,
        "--debug/--no-debug",
        help="Enable verbose debug for execution data parsing. If not specified, uses DEBUG from config/env.",
    ),
    attempt_decompress: bool = typer.Option(
        False,
        "--attempt-decompress/--no-attempt-decompress",
        help="Attempt decompression of execution data payloads. If not specified, uses ATTEMPT_DECOMPRESS from config/env.",
    ),
    debug_dump_dir: Optional[str] = typer.Option(
        None, help="Directory to dump raw execution data JSON when debug enabled (overrides DEBUG_DUMP_DIR)"
    ),
    truncate_len: Optional[int] = typer.Option(
        None,
        help="Override truncation length for input/output serialization (0 disables truncation). Overrides TRUNCATE_FIELD_LEN env setting.",
    ),
    require_execution_metadata: bool = typer.Option(
        False,
        "--require-execution-metadata/--no-require-execution-metadata",
        help=(
            "If set, only process executions that have a metadata row (execution_metadata) "
            "with key='executionId' and value matching the execution id. "
            "If not specified, uses REQUIRE_EXECUTION_METADATA from config/env."
        ),
    ),
    export_queue_soft_limit: Optional[int] = typer.Option(
        None,
        help="Override EXPORT_QUEUE_SOFT_LIMIT (approx backlog spans before applying sleep)",
    ),
    export_sleep_ms: Optional[int] = typer.Option(
        None,
        help="Override EXPORT_SLEEP_MS (sleep duration in ms when backlog exceeds soft limit)",
    ),
    filter_ai_only: bool = typer.Option(
        False,
        "--filter-ai-only/--no-filter-ai-only",
        help=(
            "Only export spans for AI-related nodes (LangChain package). Root span always "
            "included; non-AI parents of AI nodes preserved. Executions with no AI nodes "
            "export root span only with n8n.filter.no_ai_spans=true. "
            "If not specified, uses FILTER_AI_ONLY from config/env."
        ),
    ),
    skip_no_ai_spans: bool = typer.Option(
        False,
        "--skip-no-ai-spans/--no-skip-no-ai-spans",
        help=(
            "If true, do not export ANY data for traces that contain zero AI-related spans. "
            "Checkpointing still occurs. Effective even if filter-ai-only is false."
            "If not specified, uses SKIP_NO_AI_SPANS from config/env."
        ),
    ),
    poll_interval: Optional[int] = typer.Option(
        None,
        help="Seconds to sleep when no new executions found. If 0 (default), exit after batch. Overrides POLL_INTERVAL.",
    ),
) -> None:
    """Run a shipper cycle to process and export n8n executions.

    This command orchestrates the ETL process:
    - Determines the starting execution ID from the checkpoint or CLI argument.
    - Streams execution records from the database.
    - For each record, maps it to a Langfuse trace and exports it.
    - Updates the checkpoint with the ID of the last processed record.
    """
    settings = get_settings()
    logging.basicConfig(level=settings.LOG_LEVEL)
    if not os.getenv("SUPPRESS_SHIPPER_CREDIT"):
        typer.echo("Powered by n8n-langfuse-shipper (Apache 2.0) - https://github.com/rwb-truelime/n8n-langfuse-shipper")
    typer.echo("Starting shipper with mapping...")

    # Check sys.argv to detect if flags were explicitly provided
    # This allows respecting .env settings when flags are omitted
    import sys

    dry_run_explicit = "--dry-run" in sys.argv or "--no-dry-run" in sys.argv
    debug_explicit = "--debug" in sys.argv or "--no-debug" in sys.argv
    decompress_explicit = "--attempt-decompress" in sys.argv or "--no-attempt-decompress" in sys.argv

    effective_dry_run = dry_run if dry_run_explicit else settings.DRY_RUN
    effective_debug = debug if debug_explicit else settings.DEBUG
    effective_decompress = attempt_decompress if decompress_explicit else settings.ATTEMPT_DECOMPRESS
    effective_dump_dir = debug_dump_dir or settings.DEBUG_DUMP_DIR

    # Apply optional runtime overrides for export backpressure tuning
    if export_queue_soft_limit is not None:
        # Runtime override of settings attribute (present on Settings model)
        settings.EXPORT_QUEUE_SOFT_LIMIT = int(export_queue_soft_limit)
    if export_sleep_ms is not None:
        settings.EXPORT_SLEEP_MS = int(export_sleep_ms)

    # Check sys.argv for filter-ai-only and require-execution-metadata flags
    filter_ai_explicit = "--filter-ai-only" in sys.argv or "--no-filter-ai-only" in sys.argv
    skip_no_ai_explicit = "--skip-no-ai-spans" in sys.argv or "--no-skip-no-ai-spans" in sys.argv
    require_meta_explicit = "--require-execution-metadata" in sys.argv or "--no-require-execution-metadata" in sys.argv

    effective_filter_ai_only = filter_ai_only if filter_ai_explicit else settings.FILTER_AI_ONLY
    effective_skip_no_ai = skip_no_ai_spans if skip_no_ai_explicit else settings.SKIP_NO_AI_SPANS
    require_meta_flag = require_execution_metadata if require_meta_explicit else settings.REQUIRE_EXECUTION_METADATA
    effective_poll_interval = poll_interval if poll_interval is not None else settings.POLL_INTERVAL

    source = ExecutionSource(
        settings.PG_DSN,
        batch_size=settings.FETCH_BATCH_SIZE,
        schema=settings.DB_POSTGRESDB_SCHEMA or None,
        table_prefix=settings.DB_TABLE_PREFIX if settings.DB_TABLE_PREFIX is not None else None,
        require_execution_metadata=require_meta_flag,
        filter_workflow_ids=settings.FILTER_WORKFLOW_IDS,
    )

    cp_path = checkpoint_file or settings.CHECKPOINT_FILE
    effective_start_after_id = start_after_id
    effective_start_after_stopped_at: Optional[str] = None
    
    if effective_start_after_id is None:
        loaded_ts, loaded_id = load_checkpoint(cp_path)
        if loaded_id is not None:
            effective_start_after_id = loaded_id
            effective_start_after_stopped_at = loaded_ts
            logging.getLogger(__name__).info(
                "Loaded checkpoint cursor: stoppedAt=%s id=%s from %s", 
                effective_start_after_stopped_at, effective_start_after_id, cp_path
            )

    async def _run() -> None:
        # Load checkpoint once
        last_id: Optional[int] = effective_start_after_id
        last_stopped_at: Optional[str] = effective_start_after_stopped_at
        
        # Track earliest and latest startedAt among processed executions for user reconciliation.
        earliest_started: Optional[datetime] = None
        latest_started: Optional[datetime] = None

        while True:
            count_this_batch: int = 0
            
            async for raw in source.stream(
                start_after_id=last_id, 
                start_after_stopped_at=last_stopped_at, 
                limit=limit
            ):
                try:
                    record = N8nExecutionRecord(
                        id=raw["id"],
                        workflowId=raw["workflowId"],
                        status=raw["status"],
                        startedAt=raw["startedAt"],
                        stoppedAt=raw["stoppedAt"],
                        workflowData=WorkflowData(**raw["workflowData"]),
                        # Attempt to parse full execution data (with runData). Fallback to empty if shape unexpected.
                        data=_build_execution_data(
                            raw.get("data"),
                            workflow_data_raw=raw.get("workflowData"),
                            debug=effective_debug,
                            attempt_decompress=effective_decompress,
                            execution_id=raw["id"],
                        ),
                    )
                    if effective_debug and effective_dump_dir:
                        try:
                            import json
                            import os as _os
                            _os.makedirs(effective_dump_dir, exist_ok=True)
                            dump_path = _os.path.join(effective_dump_dir, f"execution_{record.id}_data.json")
                            with open(dump_path, "w", encoding="utf-8") as f:
                                json.dump(raw.get("data"), f, ensure_ascii=False, indent=2)
                            logging.getLogger(__name__).info("Dumped raw data JSON to %s", dump_path)
                        except Exception as e:
                            logging.getLogger(__name__).warning("Failed dumping raw data JSON: %s", e)
                    effective_trunc: Optional[int] = (
                        settings.TRUNCATE_FIELD_LEN if truncate_len is None else truncate_len
                    )
                    if effective_trunc == 0:
                        effective_trunc = None  # signal no truncation
                    # Media upload feature path (Langfuse Media API).
                    # Phase order change: we first export spans to obtain OTLP span ids
                    # (observation ids) then run media upload so create_media can link
                    # assets to observations. Tokens patched locally after export; the
                    # OTLP-exported span output may not include tokens (contract
                    # update documented in instructions & README).
                    mapped = None  # for media upload path later
                    if settings.ENABLE_MEDIA_UPLOAD:
                        mapped = map_execution_with_assets(
                            record,
                            truncate_limit=effective_trunc,
                            collect_binaries=True,
                            filter_ai_only=effective_filter_ai_only,
                        )
                        trace = mapped.trace
                    else:
                        trace = map_execution_to_langfuse(
                            record,
                            truncate_limit=effective_trunc,
                            filter_ai_only=effective_filter_ai_only,
                        )
                    span_count = len(trace.spans)
                    if span_count <= 1:
                        logging.getLogger(__name__).warning(
                            "Execution %s produced %d span(s); likely missing runData. workflowId=%s", record.id, span_count, record.workflowId
                        )
                    else:
                        logging.getLogger(__name__).debug(
                            "Execution %s mapped to %d spans", record.id, span_count
                        )

                    # Skip export if configured to ignore traces without AI spans.
                    # Logic: 
                    # 1. If filter_ai_only was True, the root span metadata already has 'n8n.filter.no_ai_spans' set if no AI found.
                    # 2. If filter_ai_only was False, we must scan the trace manually for any AI spans.
                    skip_export = False
                    if effective_skip_no_ai:
                        if effective_filter_ai_only:
                            # Rely on mapper metadata
                            root_span = trace.spans[0]
                            if root_span.metadata.get("n8n.filter.no_ai_spans"):
                                skip_export = True
                        else:
                            # Manual scan: check for generation type OR ai node type in metadata
                            has_ai = False
                            for s in trace.spans:
                                # Root span is not an AI span itself usually, but check all.
                                if s.observation_type == "generation":
                                    has_ai = True; break
                                # Check metadata for node type classification (requires importing is_ai_node or trust metadata)
                                # The mapper populates n8n.node.type and n8n.node.category.
                                # We can reuse is_ai_node from observation_mapper here or trust existing metadata.
                                # Note: observation_mapper is not fully imported in __main__.
                                # Let's import it locally to be safe.
                                from .observation_mapper import is_ai_node as _is_ai_node
                                
                                ntype = s.metadata.get("n8n.node.type")
                                ncat = s.metadata.get("n8n.node.category")
                                if _is_ai_node(ntype, ncat):
                                    has_ai = True; break
                            
                            if not has_ai:
                                skip_export = True

                    if skip_export:
                        logging.getLogger(__name__).info(
                            "Skipping export of execution %s: No AI spans found (SKIP_NO_AI_SPANS=True)", record.id
                        )
                    else:
                        export_trace(
                            trace,
                            settings,
                            dry_run=effective_dry_run,
                            langfuse_trace_id_field_name=settings.LANGFUSE_TRACE_ID_FIELD_NAME,
                        )
                        if settings.ENABLE_MEDIA_UPLOAD and mapped is not None:
                            # Now that OTLP span ids are populated, perform media create + upload.
                            try:
                                patch_and_upload_media(mapped, settings)
                            except Exception as e:  # pragma: no cover - non-fatal path
                                logging.getLogger(__name__).warning(
                                    "media upload phase failed execution=%s err=%s", record.id, e
                                )
                        # Track earliest / latest window for user reconciliation with Langfuse UI filters.
                        if earliest_started is None or record.startedAt < earliest_started:
                            earliest_started = record.startedAt
                        if latest_started is None or record.startedAt > latest_started:
                            latest_started = record.startedAt
                        if debug:
                            logging.getLogger(__name__).info(
                                "Exported execution %s -> trace %s spans=%d startedAt=%s",
                                record.id,
                                trace.id,
                                len(trace.spans),
                                record.startedAt.isoformat(),
                            )
                except Exception as e:
                    # Catch processing errors to prevent restart loops (which cause duplication)
                    # Log error with full context and skip this record
                    logging.getLogger(__name__).error(
                        "Skipping execution %s due to processing error: %s",
                        raw.get("id"),
                        e,
                        exc_info=True,
                    )
                
                count_this_batch += 1
                # Update cursor even on error to ensure we advance past the bad record
                last_id = int(raw["id"])
                if raw.get("stoppedAt"):
                    last_stopped_at = raw["stoppedAt"].isoformat()

                # Periodic checkpointing for long-running stream safety.
                # Strategy: Only checkpoint when we are sure the OTLP exporter has flushed the data
                # (count is multiple of FLUSH_EVERY_N_TRACES) AND we have processed a reasonable
                # batch (e.g. >= 50) to avoid excessive disk I/O.
                if not effective_dry_run and last_id is not None:
                    flush_n = max(1, settings.FLUSH_EVERY_N_TRACES)
                    # Ensure checkpoint_n is a multiple of flush_n and >= 50
                    min_batch = 50
                    if flush_n >= min_batch:
                        checkpoint_n = flush_n
                    else:
                        # Round up min_batch to next multiple of flush_n
                        checkpoint_n = ((min_batch + flush_n - 1) // flush_n) * flush_n
                    
                    if count_this_batch % checkpoint_n == 0:
                        store_checkpoint(cp_path, last_id, last_stopped_at)
                        logging.getLogger(__name__).debug("Stored periodic checkpoint %s|%s", last_stopped_at, last_id)

            # End of batch/stream loop
            if not effective_dry_run and last_id is not None and count_this_batch > 0:
                store_checkpoint(cp_path, last_id, last_stopped_at)
                logging.getLogger(__name__).info(
                    "Stored checkpoint %s|%s to %s", last_stopped_at, last_id, cp_path
                )

            typer.echo(
                f"Batch processed {count_this_batch} execution(s). dry_run={effective_dry_run} cursor={last_id}"
            )
            
            if count_this_batch > 0:
                logging.getLogger(__name__).info(
                    (
                        "Execution time window processed: earliest_started=%s "
                        "latest_started=%s (UTC). If Langfuse UI date filter excludes part of "
                        "this range, displayed trace count may be lower."
                    ),
                    earliest_started.isoformat() if earliest_started else None,
                    latest_started.isoformat() if latest_started else None,
                )

            # Polling logic
            if effective_poll_interval > 0:
                if limit is None or count_this_batch < limit:
                    logging.getLogger(__name__).info(
                        "Caught up (count=%d). Sleeping %ds...", count_this_batch, effective_poll_interval
                    )
                    await asyncio.sleep(effective_poll_interval)
                else:
                    # Hit limit, yield briefly then continue
                    await asyncio.sleep(0.1)
            else:
                logging.getLogger(__name__).info("Shipper cycle completed (single-run mode).")
                break

    asyncio.run(_run())
    # Ensure exporter flush & shutdown for short-lived process reliability
    shutdown_exporter()


if __name__ == "__main__":  # pragma: no cover
    app()
