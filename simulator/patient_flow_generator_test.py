import json
import random
import uuid
import time
from datetime import datetime, timedelta, timezone
# from kafka import KafkaProducer
import csv  # Thêm
import os   # Thêm

# THÊM: Cấu hình file đầu ra
OUTPUT_FILE = "patient_data_sample.csv"
NUMBER_OF_RECORDS = 5000 # Anh muốn tạo bao nhiêu dòng thì sửa ở đây

# #Eventhub Configuration
# EVENTHUBS_NAMESPACE = "<<NAMESPACE_HOSTNAME>>"
# EVENT_HUB_NAME="<<EVENT_HUB_NAME>>"  
# CONNECTION_STRING = "<<NAMESPACE_CONNECTION_STRING>>"

# producer = KafkaProducer(
#     bootstrap_servers=[f"{EVENTHUBS_NAMESPACE}:9093"],
#     security_protocol="SASL_SSL",
#     sasl_mechanism="PLAIN",
#     sasl_plain_username="$ConnectionString",
#     sasl_plain_password=CONNECTION_STRING,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# Departments in hospital
departments = ["Emergency", "Surgery", "ICU", "Pediatrics", "Maternity", "Oncology", "Cardiology"]

# Gender categories 
genders = ["Male", "Female"]

# Helper function to introduce dirty data
def inject_dirty_data(record):

    # 5% chance to have invalid age
    if random.random() < 0.05:
        record["age"] = random.randint(101, 150)

    # 5% chance to have future admission timestamp
    if random.random() < 0.05:
        record["admission_time"] = (datetime.now(timezone.utc)+ timedelta(hours=random.randint(1, 72))).isoformat()

    return record

def generate_patient_event():
    admission_time = datetime.now(timezone.utc) - timedelta(hours=random.randint(0, 72))
    discharge_time = admission_time + timedelta(hours=random.randint(1, 72))

    event = {
        "patient_id": str(uuid.uuid4()),
        "gender": random.choice(genders),
        "age": random.randint(1, 100),
        "department": random.choice(departments),
        "admission_time": admission_time.isoformat(),
        "discharge_time": discharge_time.isoformat(),
        "bed_id": random.randint(1, 500),
        "hospital_id": random.randint(1, 7)  # Assuming 7 hospitals in network
    }

    return inject_dirty_data(event)

# if __name__ == "__main__":
#     while True:
#         event = generate_patient_event()
#         producer.send(EVENT_HUB_NAME, event)
#         print(f"Sent to Event Hub: {event}")
#         time.sleep(1)

if __name__ == "__main__":
    print(f"Đang khởi tạo dữ liệu vào file {OUTPUT_FILE}...")

    # Lấy danh sách các cột từ 1 event mẫu
    fieldnames = list(generate_patient_event().keys())

    # Mở file CSV để ghi
    with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader() # Ghi dòng tiêu đề (Header)

        for i in range(NUMBER_OF_RECORDS):
            event = generate_patient_event()
            writer.writerow(event)
            if (i+1) % 10 == 0:
                print(f"Đã tạo {i+1} dòng...")

    print(f"--- THÀNH CÔNG ---")
    print(f"File lưu tại: {os.path.abspath(OUTPUT_FILE)}") 