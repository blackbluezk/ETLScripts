import json
import csv
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

INPUT_DIR = "/opt/airflow/data/input"
OUTPUT_DIR = "/opt/airflow/data/output"

PATIENT_FIELDS = [
    "patient_id", "patient_uuid", "first_name", "last_name", "gender",
    "birthdate", "phone_number", "address1", "city_village",
    "state_province", "country", "datim_id",
]

VISIT_FIELDS = [
    "visit_id", "visit_uuid", "patient_id", "patient_uuid",
    "location_id", "visit_type_id", "date_started", "date_stopped",
]

ENCOUNTER_FIELDS = [
    "encounter_id", "encounter_uuid", "encounter_type_id", "patient_id",
    "patient_uuid", "location_id", "visit_id", "pmm_form", "encounter_datetime",
]

OBSERVATION_FIELDS = [
    "obs_id", "obs_uuid", "person_id", "patient_uuid", "concept_id",
    "encounter_id", "obs_datetime", "value_numeric", "value_text",
    "variable_name", "variable_value", "datatype", "datim_id",
]


def extract_records():
    patients, visits, encounters, observations = [], [], [], []

    for filename in os.listdir(INPUT_DIR):
        if not filename.endswith(".json"):
            continue

        filepath = os.path.join(INPUT_DIR, filename)
        with open(filepath, "r") as f:
            data = json.load(f)

        if isinstance(data, dict):
            data = [data]

        for obj in data:
            msgdata = obj.get("messageData", {})

            demographics = msgdata.get("demographics") or {}
            if demographics.get("patientUuid"):
                patients.append({
                    "patient_id": demographics.get("patientId"),
                    "patient_uuid": demographics.get("patientUuid"),
                    "first_name": demographics.get("firstName"),
                    "last_name": demographics.get("lastName"),
                    "gender": demographics.get("gender"),
                    "birthdate": demographics.get("birthdate"),
                    "phone_number": demographics.get("phoneNumber"),
                    "address1": demographics.get("address1"),
                    "city_village": demographics.get("cityVillage"),
                    "state_province": demographics.get("stateProvince"),
                    "country": demographics.get("country"),
                    "datim_id": demographics.get("datimId"),
                })

            for v in msgdata.get("visits", []) or []:
                visits.append({
                    "visit_id": v.get("visitId"),
                    "visit_uuid": v.get("visitUuid"),
                    "patient_id": v.get("patientId"),
                    "patient_uuid": v.get("patientUuid"),
                    "location_id": v.get("locationId"),
                    "visit_type_id": v.get("visitTypeId"),
                    "date_started": v.get("dateStarted"),
                    "date_stopped": v.get("dateStopped"),
                })

            for e in msgdata.get("encounters", []) or []:
                encounters.append({
                    "encounter_id": e.get("encounterId"),
                    "encounter_uuid": e.get("encounterUuid"),
                    "encounter_type_id": e.get("encounterTypeId"),
                    "patient_id": e.get("patientId"),
                    "patient_uuid": e.get("patientUuid"),
                    "location_id": e.get("locationId"),
                    "visit_id": e.get("visitId"),
                    "pmm_form": e.get("pmmForm"),
                    "encounter_datetime": e.get("encounterDatetime"),
                })

            for o in msgdata.get("obs", []) or []:
                observations.append({
                    "obs_id": o.get("obsId"),
                    "obs_uuid": o.get("obsUuid"),
                    "person_id": o.get("personId"),
                    "patient_uuid": o.get("patientUuid"),
                    "concept_id": o.get("conceptId"),
                    "encounter_id": o.get("encounterId"),
                    "obs_datetime": o.get("obsDatetime"),
                    "value_numeric": o.get("valueNumeric"),
                    "value_text": o.get("valueText"),
                    "variable_name": o.get("variableName"),
                    "variable_value": o.get("variableValue"),
                    "datatype": o.get("datatype"),
                    "datim_id": o.get("datimId"),
                })

    return patients, visits, encounters, observations


def write_csv(filename, fieldnames, rows):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {filename}")


def convert_json_to_csv():
    patients, visits, encounters, observations = extract_records()

    write_csv("patients.csv", PATIENT_FIELDS, patients)
    write_csv("visits.csv", VISIT_FIELDS, visits)
    write_csv("encounters.csv", ENCOUNTER_FIELDS, encounters)
    write_csv("observations.csv", OBSERVATION_FIELDS, observations)


with DAG(
    dag_id="json_to_csv",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["conversion"],
) as dag:

    convert_task = PythonOperator(
        task_id="convert_json_to_csv",
        python_callable=convert_json_to_csv,
    )