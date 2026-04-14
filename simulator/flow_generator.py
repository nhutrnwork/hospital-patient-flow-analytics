import json
import random
import uuid
import time
import csv
import os
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# --- Event Hubs Configuration (Kafka Interface) ---
EVENTHUBS_NAMESPACE = "<<NAMESPACE_HOSTNAME>>"
EVENT_HUB_NAME="<<EVENT_HUB_NAME>>"  
CONNECTION_STRING = "<<NAMESPACECONNECTION_STRING>>"

# Initialize Producer
producer = KafkaProducer(
    bootstrap_servers=[f"{EVENTHUBS_NAMESPACE}:9093"],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Time Compression Configuration ---
REAL_SECONDS_PER_VIRTUAL_HOUR = 5  # 5 real seconds = 1 virtual hour
VIRTUAL_TIME_STEP = timedelta(hours=1)
virtual_clock = datetime.now(timezone.utc) # Global virtual clock

ERROR_RATE = 0.05

GENDERS = ["Male", "Female"]

# --- Master Data: Hospital structure and gender ratios ---
HOSPITAL_STRUCTURE = {
    1: {"Emergency": 80, "Surgery": 50, "ICU": 30, "Cardiology": 40},
    2: {"Pediatrics": 60, "Maternity": 40, "Emergency": 20},
    3: {"Oncology": 100, "Surgery": 60, "ICU": 40},
    4: {"Emergency": 30, "Cardiology": 20},
    5: {"Pediatrics": 40, "ICU": 15},
    6: {"Surgery": 30, "Maternity": 25},
    7: {"Emergency": 25, "Oncology": 20}
}

DEPT_GENDER_RATIO = {
    "Maternity": [0.05, 0.95],
    "Pediatrics": [0.5, 0.5],
    "Emergency": [0.6, 0.4],
    "Surgery": [0.7, 0.3],
    "ICU": [0.55, 0.45],
    "Oncology": [0.4, 0.6],
    "Cardiology": [0.65, 0.35]
}

# Management lists for hospitalized patients and occupied beds
active_patients = {} 
active_beds = {}     

# --- Helper: Inject dirty/erroneous data ---
def inject_dirty_data(clean_record, current_time):
    record = clean_record.copy()

    if random.random() > ERROR_RATE:
        return record

    error_type = random.choice([
        "patient_already_in_bed",
        "bed_occupied",
        "discharge_without_admission",
        "future_time",
        "dirty_age",
        "dirty_gender"
    ])

    if error_type == "patient_already_in_bed":
        if active_patients:
            record["patient_id"] = random.choice(list(active_patients.keys()))

    elif error_type == "bed_occupied":
        if active_beds:
            record["bed_id"] = random.choice(list(active_beds.keys()))

    elif error_type == "discharge_without_admission":
        record["activity_type"] = "Discharge"
        record["patient_id"] = str(uuid.uuid4())

    elif error_type == "future_time":
        record["activity_time"] = (
            current_time + timedelta(hours=random.randint(1, 72))
        ).isoformat()

    elif error_type == "dirty_age":
        record["age"] = random.choice([-5, 150])

    elif error_type == "dirty_gender":
        record["gender"] = random.choice(["M", "F", "Unknown"])

    return record

# --- Hospital Event Processing Logic ---
def process_hospital_event(event_time):
    """Process Admission/Discharge logic at a specific time point"""

    # --- DISCHARGE ---
    for pid, info in list(active_patients.items()):
        if event_time >= info["discharge_at"]:

            event = info["data"].copy()
            event["activity_type"] = "Discharge"
            event["activity_time"] = event_time.isoformat()

            del active_patients[pid]
            del active_beds[event["bed_id"]]

            return inject_dirty_data(event, event_time)

    # --- ADMISSION ---
    hosp_id = random.randint(1, 7)
    dept = random.choice(list(HOSPITAL_STRUCTURE[hosp_id].keys()))
    
    ratio = DEPT_GENDER_RATIO.get(dept, [0.5, 0.5])
    gender = random.choices(GENDERS, weights=ratio)[0]
    
    max_beds = HOSPITAL_STRUCTURE[hosp_id][dept]
    
    available_beds = []
    for b in range(1, max_beds + 1):
        bed_id = f"H{hosp_id:02d}-{dept[:3].upper()}-B{b:03d}"
        if bed_id not in active_beds:
            available_beds.append(bed_id)

    if not available_beds:
        return None

    bed_id = random.choice(available_beds)
    patient_id = str(uuid.uuid4())
    
    stay_duration = timedelta(days=random.randint(3, 10))

    admission_event = {
        "patient_id": patient_id,
        "gender": gender,
        "age": random.randint(1, 90),
        "department": dept,
        "hospital_id": hosp_id,
        "bed_id": bed_id,
        "activity_type": "Admission",
        "activity_time": event_time.isoformat()
    }

    active_beds[bed_id] = patient_id

    # Store clean version in cache to be used for future Discharge
    active_patients[patient_id] = {
        "data": admission_event.copy(),
        "discharge_at": event_time + stay_duration
    }

    # Return the Admission event with injected errors (if any)
    return inject_dirty_data(admission_event, event_time)

# --- MAIN LOOP ---
if __name__ == "__main__":
    print(f"Start simulation: {REAL_SECONDS_PER_VIRTUAL_HOUR}s = 1h virtual.")
    print(f"Simulation started. Sending data to Event Hub: {EVENT_HUB_NAME}")

    virtual_clock = datetime.now(timezone.utc) - timedelta(days=10)
    
    try:
        while True:
            real_now = datetime.now(timezone.utc)
            virtual_clock += VIRTUAL_TIME_STEP
            
            if virtual_clock > real_now:
                virtual_clock = real_now
            
            num_events = random.randint(0, 5)
            
            for _ in range(num_events):
                
                event_time = virtual_clock + timedelta(
                    minutes=random.randint(0, 59), 
                    seconds=random.randint(0, 59))
                
                event = process_hospital_event(event_time)
                if event:
                    producer.send(EVENT_HUB_NAME, event)
                    print(event)
                    
            time.sleep(REAL_SECONDS_PER_VIRTUAL_HOUR)
		
    except KeyboardInterrupt:
        print("\nStopped safely")