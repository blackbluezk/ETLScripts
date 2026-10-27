import hashlib
import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# Read-only: the DAG only ever reads from this folder. Nothing is moved,
# renamed, or deleted here. "Already processed" state lives entirely in
# the file_ingest_log table (filename + content hash + status), not in
# the filesystem, so no write access to this directory is required.
INPUT_DIR = "/opt/airflow/data/input"

DDL = """
-- This table is the ONLY source of truth for "has this file been handled?"
-- (filename + content hash + status). Files in INPUT_DIR are never moved
-- or deleted, so a run that finds many old already-succeeded files will
-- still list + hash + look each one up here every time - cheap per file,
-- but worth knowing this cost grows with how many files accumulate.
CREATE TABLE IF NOT EXISTS file_ingest_log (
    filename TEXT PRIMARY KEY,
    file_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'processing', -- processing | success | failed
    records_inserted INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMPTZ DEFAULT now(),
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS json_imports (
    id SERIAL PRIMARY KEY,
    filename TEXT,
    data JSONB,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    transformed_at TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS patients (
    patient_id INTEGER,
    patient_uuid TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    birthdate DATE,
    phone_number TEXT,
    address1 TEXT,
    city_village TEXT,
    state_province TEXT,
    country TEXT,
    datim_id TEXT,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    transformed_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS visits (
    visit_id INTEGER,
    visit_uuid TEXT PRIMARY KEY,
    patient_id INTEGER,
    patient_uuid TEXT REFERENCES patients(patient_uuid),
    location_id INTEGER,
    visit_type_id INTEGER,
    date_started TIMESTAMPTZ,
    date_stopped TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    transformed_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS encounters (
    encounter_id INTEGER,
    encounter_uuid TEXT PRIMARY KEY,
    encounter_type_id INTEGER,
    patient_id INTEGER,
    patient_uuid TEXT REFERENCES patients(patient_uuid),
    location_id INTEGER,
    visit_id INTEGER,
    pmm_form TEXT,
    encounter_datetime TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    transformed_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS observations (
    obs_id INTEGER,
    obs_uuid TEXT PRIMARY KEY,
    person_id INTEGER,
    patient_uuid TEXT REFERENCES patients(patient_uuid),
    concept_id INTEGER,
    encounter_id INTEGER,
    obs_datetime TIMESTAMPTZ,
    value_numeric NUMERIC,
    value_text TEXT,
    variable_name TEXT,
    variable_value TEXT,
    datatype TEXT,
    datim_id TEXT,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    transformed_at TIMESTAMPTZ DEFAULT now()
);
"""

PATIENT_UPSERT = """
INSERT INTO patients (
    patient_id, patient_uuid, first_name, last_name, gender, birthdate,
    phone_number, address1, city_village, state_province, country, datim_id
) VALUES %s
ON CONFLICT (patient_uuid) DO UPDATE SET
    patient_id = EXCLUDED.patient_id,
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    gender = EXCLUDED.gender,
    birthdate = EXCLUDED.birthdate,
    phone_number = EXCLUDED.phone_number,
    address1 = EXCLUDED.address1,
    city_village = EXCLUDED.city_village,
    state_province = EXCLUDED.state_province,
    country = EXCLUDED.country,
    datim_id = EXCLUDED.datim_id,
    transformed_at = now();
"""

VISIT_UPSERT = """
INSERT INTO visits (
    visit_id, visit_uuid, patient_id, patient_uuid, location_id,
    visit_type_id, date_started, date_stopped
) VALUES %s
ON CONFLICT (visit_uuid) DO UPDATE SET
    visit_id = EXCLUDED.visit_id,
    patient_id = EXCLUDED.patient_id,
    patient_uuid = EXCLUDED.patient_uuid,
    location_id = EXCLUDED.location_id,
    visit_type_id = EXCLUDED.visit_type_id,
    date_started = EXCLUDED.date_started,
    date_stopped = EXCLUDED.date_stopped,
    transformed_at = now();
"""

ENCOUNTER_UPSERT = """
INSERT INTO encounters (
    encounter_id, encounter_uuid, encounter_type_id, patient_id, patient_uuid,
    location_id, visit_id, pmm_form, encounter_datetime
) VALUES %s
ON CONFLICT (encounter_uuid) DO UPDATE SET
    encounter_id = EXCLUDED.encounter_id,
    encounter_type_id = EXCLUDED.encounter_type_id,
    patient_id = EXCLUDED.patient_id,
    patient_uuid = EXCLUDED.patient_uuid,
    location_id = EXCLUDED.location_id,
    visit_id = EXCLUDED.visit_id,
    pmm_form = EXCLUDED.pmm_form,
    encounter_datetime = EXCLUDED.encounter_datetime,
    transformed_at = now();
"""

OBSERVATION_UPSERT = """
INSERT INTO observations (
    obs_id, obs_uuid, person_id, patient_uuid, concept_id, encounter_id,
    obs_datetime, value_numeric, value_text, variable_name, variable_value,
    datatype, datim_id
) VALUES %s
ON CONFLICT (obs_uuid) DO UPDATE SET
    obs_id = EXCLUDED.obs_id,
    person_id = EXCLUDED.person_id,
    patient_uuid = EXCLUDED.patient_uuid,
    concept_id = EXCLUDED.concept_id,
    encounter_id = EXCLUDED.encounter_id,
    obs_datetime = EXCLUDED.obs_datetime,
    value_numeric = EXCLUDED.value_numeric,
    value_text = EXCLUDED.value_text,
    variable_name = EXCLUDED.variable_name,
    variable_value = EXCLUDED.variable_value,
    datatype = EXCLUDED.datatype,
    datim_id = EXCLUDED.datim_id,
    transformed_at = now();
"""


def _file_hash(filepath: str) -> str:
    """Content hash so a re-dropped file with the same name but different
    content is still (re)processed, while an untouched file is skipped."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _to_numeric(value):
    """Coerce to a Postgres-safe numeric input; blank/invalid -> NULL
    instead of crashing the whole file's transaction."""
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        logger.warning("Non-numeric valueNumeric %r; storing as NULL", value)
        return None


@contextmanager
def _pg_connection(hook: PostgresHook):
    """Guarantee the connection (not just the cursor) is always released,
    even on exceptions."""
    conn = hook.get_conn()
    try:
        yield conn
    finally:
        conn.close()


def _extract_rows(msgdata: dict):
    """Pull child records out of one message, skipping/logging any record
    missing the primary-key field it needs rather than letting Postgres
    raise a NotNullViolation mid-transaction."""
    demographics = msgdata.get("demographics") or {}
    patient_row = None
    patient_uuid = demographics.get("patientUuid")
    if patient_uuid:
        patient_row = (
            demographics.get("patientId"),
            patient_uuid,
            demographics.get("firstName"),
            demographics.get("lastName"),
            demographics.get("gender"),
            demographics.get("birthdate") or None,
            demographics.get("phoneNumber"),
            demographics.get("address1"),
            demographics.get("cityVillage"),
            demographics.get("stateProvince"),
            demographics.get("country"),
            demographics.get("datimId"),
        )
    else:
        logger.warning("Skipping demographics block with no patientUuid")

    visit_rows = []
    for v in msgdata.get("visits", []) or []:
        if not v.get("visitUuid"):
            logger.warning("Skipping visit with no visitUuid: %r", v.get("visitId"))
            continue
        visit_rows.append((
            v.get("visitId"), v.get("visitUuid"), v.get("patientId"),
            v.get("patientUuid"), v.get("locationId"), v.get("visitTypeId"),
            v.get("dateStarted") or None, v.get("dateStopped") or None,
        ))

    encounter_rows = []
    for e in msgdata.get("encounters", []) or []:
        if not e.get("encounterUuid"):
            logger.warning("Skipping encounter with no encounterUuid: %r", e.get("encounterId"))
            continue
        encounter_rows.append((
            e.get("encounterId"), e.get("encounterUuid"), e.get("encounterTypeId"),
            e.get("patientId"), e.get("patientUuid"), e.get("locationId"),
            e.get("visitId"), e.get("pmmForm"), e.get("encounterDatetime") or None,
        ))

    obs_rows = []
    for o in msgdata.get("obs", []) or []:
        if not o.get("obsUuid"):
            logger.warning("Skipping obs with no obsUuid: %r", o.get("obsId"))
            continue
        obs_rows.append((
            o.get("obsId"), o.get("obsUuid"), o.get("personId"), o.get("patientUuid"),
            o.get("conceptId"), o.get("encounterId"), o.get("obsDatetime") or None,
            _to_numeric(o.get("valueNumeric")), o.get("valueText"),
            o.get("variableName"), o.get("variableValue"), o.get("datatype"),
            o.get("datimId"),
        ))

    return patient_row, visit_rows, encounter_rows, obs_rows


def insert_json_to_postgres(**context):
    hook = PostgresHook(postgres_conn_id="pipeline_postgres")
    hook.run(DDL)

    # Deterministic order: files are processed oldest-name-first so that,
    # if two files touch the same entity, the "later" one applied last wins
    # in a predictable, reproducible way (instead of OS-dependent listdir order).
    filenames = sorted(f for f in os.listdir(INPUT_DIR) if f.endswith(".json"))

    for filename in filenames:
        filepath = os.path.join(INPUT_DIR, filename)
        if not os.path.isfile(filepath):
            continue

        file_hash = _file_hash(filepath)

        # Skip files already successfully processed with this exact content.
        # This is the ONLY thing that decides "already done" - the file
        # itself is never moved, renamed, or deleted, so no write access
        # to INPUT_DIR is needed.
        already_done = hook.get_first(
            "SELECT 1 FROM file_ingest_log WHERE filename = %s AND file_hash = %s AND status = 'success'",
            parameters=(filename, file_hash),
        )
        if already_done:
            logger.info("Skipping %s (already processed, unchanged)", filename)
            continue

        try:
            with open(filepath, "r") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Cannot read/parse %s: %s", filename, exc)
            _mark_failed(hook, filename, file_hash, str(exc))
            continue

        if isinstance(data, dict):
            data = [data]
        if not data:
            continue

        try:
            records_inserted = _process_file(hook, filename, data)
        except Exception as exc:  # noqa: BLE001 - log and move on, don't kill the whole run
            logger.exception("Failed processing %s", filename)
            _mark_failed(hook, filename, file_hash, str(exc))
            continue

        hook.run(
            """
            INSERT INTO file_ingest_log (filename, file_hash, status, records_inserted, finished_at)
            VALUES (%s, %s, 'success', %s, now())
            ON CONFLICT (filename) DO UPDATE SET
                file_hash = EXCLUDED.file_hash,
                status = 'success',
                records_inserted = EXCLUDED.records_inserted,
                error_message = NULL,
                finished_at = now();
            """,
            parameters=(filename, file_hash, records_inserted),
        )
        logger.info("Processed %s: %s raw records", filename, records_inserted)


def _mark_failed(hook: PostgresHook, filename: str, file_hash: str, error_message: str):
    hook.run(
        """
        INSERT INTO file_ingest_log (filename, file_hash, status, error_message, finished_at)
        VALUES (%s, %s, 'failed', %s, now())
        ON CONFLICT (filename) DO UPDATE SET
            file_hash = EXCLUDED.file_hash,
            status = 'failed',
            error_message = EXCLUDED.error_message,
            finished_at = now();
        """,
        parameters=(filename, file_hash, error_message[:2000]),
    )


def _process_file(hook: PostgresHook, filename: str, data: list) -> int:
    patient_rows, visit_rows, encounter_rows, obs_rows = [], [], [], []
    raw_rows = []

    for obj in data:
        raw_rows.append((filename, json.dumps(obj)))
        msgdata = obj.get("messageData", {})
        patient_row, v_rows, e_rows, o_rows = _extract_rows(msgdata)
        if patient_row:
            patient_rows.append(patient_row)
        visit_rows.extend(v_rows)
        encounter_rows.extend(e_rows)
        obs_rows.extend(o_rows)

    # Single transaction per file: either the whole file lands, or none of it
    # does, and it gets quarantined to /failed for inspection instead of
    # silently retrying forever.
    with _pg_connection(hook) as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO json_imports (filename, data) VALUES %s",
                raw_rows,
            )
            if patient_rows:
                execute_values(cur, PATIENT_UPSERT, patient_rows)
            if visit_rows:
                execute_values(cur, VISIT_UPSERT, visit_rows)
            if encounter_rows:
                execute_values(cur, ENCOUNTER_UPSERT, encounter_rows)
            if obs_rows:
                execute_values(cur, OBSERVATION_UPSERT, obs_rows)

            cur.execute(
                "UPDATE json_imports SET transformed_at = now() "
                "WHERE filename = %s AND transformed_at IS NULL",
                (filename,),
            )
        conn.commit()

    return len(raw_rows)


with DAG(
    dag_id="json_to_postgresql",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["conversion"],
) as dag:

    convert_task = PythonOperator(
        task_id="convert_json_to_postgresql",
        python_callable=insert_json_to_postgres,
    )