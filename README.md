#!/usr/bin/env python3
"""
generate_healthcare_dataset_600k.py

Creates ONE flat CSV with 600,000 visit-level rows for a healthcare domain.
- Includes 25 DIMENSIONS (each with ID + Name) repeated across rows (foreign-key style).
- Simulates patient revisits (same PatientID on multiple visits).
- Simulates role switching (Doctor also Staff; Staff acting as Doctor sometimes).
- Adds hospital-level attributes.

Output: healthcare_dataset.csv
"""

import csv
import random
from datetime import datetime, timedelta
from faker import Faker

# -----------------------------
# CONFIG
# -----------------------------
SEED = 42
TOTAL_ROWS = 600_000             # updated from 5000 to 6 lakh
UNIQUE_PATIENTS = 3200
UNIQUE_DOCTORS = 180
UNIQUE_STAFF = 300
UNIQUE_BEDS = 450
UNIQUE_EQUIPMENT = 220
UNIQUE_MEDICINES = 400
UNIQUE_HOSPITALS = 150

P_DOCTOR_ALSO_STAFF = 0.12
P_STAFF_ACTING_AS_DOCTOR = 0.08
P_ADMISSION = 0.35
P_DISCHARGE_SAME_DAY = 0.65
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 8, 1)

random.seed(SEED)
faker = Faker()
Faker.seed(SEED)

# -----------------------------
# MASTER DIMENSIONS (25 total)
# -----------------------------
def make_pairs(prefix, names):
    return [(i+1, n) for i, n in enumerate(names)]

Gender = make_pairs("Gender", ["Male", "Female", "Other"])
States = make_pairs("State", [
    "Maharashtra","Karnataka","Gujarat","Delhi","Tamil Nadu","Telangana","West Bengal","Rajasthan",
    "Madhya Pradesh","Uttar Pradesh","Punjab","Haryana","Kerala","Bihar","Odisha"
])
Cities = make_pairs("City", [
    "Mumbai","Pune","Nagpur","Bengaluru","Mysuru","Ahmedabad","Surat","Delhi","Chennai","Hyderabad",
    "Kolkata","Jaipur","Indore","Lucknow","Chandigarh","Kochi","Patna","Bhubaneswar"
])
Education = make_pairs("Education", ["MBBS","MD","MS","DNB","BAMS","BHMS","MCh","DM","PhD"])
Specialization = make_pairs("Specialization", [
    "General Medicine","Cardiology","Orthopedics","Neurology","Pediatrics","Dermatology",
    "ENT","Gynecology","Oncology","Gastroenterology","Psychiatry","Pulmonology","Nephrology"
])
EmploymentType = make_pairs("EmploymentType", ["Permanent","Visiting","Contract"])
Shift = make_pairs("Shift", ["Morning","Evening","Night"])
StaffRole = make_pairs("StaffRole", ["Nurse","Receptionist","Technician","Pharmacist","Cleaner","Security","Admin","Doctor"])
Department = make_pairs("Department", [
    "Emergency","ICU","General Ward","OPD","Radiology","Pathology","Surgery","Cardiology",
    "Orthopedics","Pediatrics","Gynecology","Oncology","Neurology"
])
BedType = make_pairs("BedType", ["ICU","General","Private","Semi-Private","Pediatric"])
BedStatus = make_pairs("BedStatus", ["Occupied","Available","Maintenance","Reserved"])
EquipmentType = make_pairs("EquipmentType", ["MRI","CT Scan","X-Ray","Ventilator","ECG","Ultrasound","Infusion Pump","Defibrillator","Surgical Light"])
EquipmentCondition = make_pairs("EquipmentCondition", ["Good","Needs Repair","Out of Service","Replaced"])
MedicineUsage = make_pairs("MedicineUsage", ["Fever","Headache","Stomach Pain","Cold & Cough","Infection","Hypertension","Diabetes","Body Pain"])
DosageStrength = make_pairs("DosageStrength", ["50 mg","100 mg","200 mg","250 mg","500 mg"])
MedicineType = make_pairs("MedicineType", ["Tablet","Capsule","Injection","Insulin","Tonic","Syrup","Drops"])
MedicineBrand = make_pairs("MedicineBrand", ["Cipla","Sun Pharma","Dr. Reddy's","Pfizer","GSK","Abbott","Lupin","Aurobindo"])
MedicineCategory = make_pairs("MedicineCategory", ["Painkiller","Antibiotic","Antifungal","Antiviral","Antipyretic","Antihypertensive","Antidiabetic"])
MedicineColour = make_pairs("MedicineColour", ["White","Red","Blue","Green","Yellow","Pink","Orange"])
InsuranceProvider = make_pairs("InsuranceProvider", ["Star Health","ICICI Lombard","HDFC Ergo","New India","Bajaj Allianz","United India","SBI General"])
PaymentMethod = make_pairs("PaymentMethod", ["Cash","Card","UPI","Insurance"])
PaymentStatus = make_pairs("PaymentStatus", ["Paid","Pending","Partially Paid","Rejected"])
VisitReason = make_pairs("VisitReason", ["Check-up","Follow-up","Emergency","Surgery","Diagnosis","Vaccination","Therapy Session"])
Diagnosis = make_pairs("Diagnosis", [
    "Viral Fever","Migraine","Gastritis","Fracture","Asthma","Hypertension","Diabetes","Dermatitis",
    "Otitis Media","Anemia","COVID-19","Pneumonia","UTI"
])
TestType = make_pairs("TestType", ["Blood Test","X-Ray","MRI","CT Scan","ECG","Ultrasound","Urine Test","Spirometry"])

# Hospital Specialization
HospitalSpecialization = [
    "Multi-Specialty", "Cardiac", "Pediatric", "Cancer Care", "Neurology Center", "Orthopedic",
    "Maternity", "Eye Care", "ENT", "General"
]

GovPrivate = ["Government", "Private"]

# -----------------------------
# Entity ID pools
# -----------------------------
def make_code(prefix, n, width=5):
    return [f"{prefix}{str(i+1).zfill(width)}" for i in range(n)]

PatientIDs = make_code("PAT", UNIQUE_PATIENTS)
DoctorIDs = make_code("DOC", UNIQUE_DOCTORS)
StaffIDs = make_code("STF", UNIQUE_STAFF)
BedIDs = make_code("BED", UNIQUE_BEDS)
EquipmentIDs = make_code("EQP", UNIQUE_EQUIPMENT)
MedicineIDs = make_code("MED", UNIQUE_MEDICINES)
HospitalIDs = make_code("HOS", UNIQUE_HOSPITALS)

# -----------------------------
# Profiles for Hospitals
# -----------------------------
HospitalProfiles = [
    {
        "HospitalID": h,
        "HospitalName": f"{faker.last_name()} Hospital",
        "HospitalLocation": random.choice(Cities)[1],
        "HospitalAddress": faker.address().replace("\n", ", "),
        "GovPrivate": random.choice(GovPrivate),
        "HospitalSpecialization": random.choice(HospitalSpecialization)
    }
    for h in HospitalIDs
]

# Map hospital by ID for lookup
HospitalByID = {h["HospitalID"]: h for h in HospitalProfiles}

# -----------------------------
# Patient Registry
# -----------------------------
PatientRegistry = {}
for pid in PatientIDs:
    fname = faker.first_name()
    lname = faker.last_name()
    PatientRegistry[pid] = {
        "PatientID": pid,
        "FirstName": fname,
        "LastName": lname,
        "FullName": f"{fname} {lname}",
        "Gender": random.choice(Gender),
        "DOB": faker.date_between(start_date="-90y", end_date="-1y"),
        "State": random.choice(States),
        "City": random.choice(Cities),
        "Phone": faker.msisdn(),
        "Email": faker.email(),
        "BloodGroup": random.choice(["A+","A-","B+","B-","AB+","AB-","O+","O-"])
    }

# -----------------------------
# Helper functions
# -----------------------------
def pick(pairs):
    return random.choice(pairs)

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days),
                             seconds=random.randint(0, 86399))

def days_between(a, b):
    return (b - a).days

def choose_patient_id():
    cut = int(0.5 * UNIQUE_PATIENTS)
    if random.random() < 0.6:
        return random.choice(PatientIDs[:cut])
    return random.choice(PatientIDs[cut:])

def choose_visit_dates():
    visit_date = random_date(START_DATE, END_DATE)
    is_admit = random.random() < P_ADMISSION
    admit_date = visit_date if is_admit else None
    discharge_date = None
    if is_admit:
        if random.random() < P_DISCHARGE_SAME_DAY:
            discharge_date = admit_date
        else:
            discharge_date = admit_date + timedelta(days=random.randint(1, 14))
    return visit_date, is_admit, admit_date, discharge_date

# -----------------------------
# Generate rows
# -----------------------------
rows = []
for ridx in range(1, TOTAL_ROWS + 1):
    record_id = f"VIS{str(ridx).zfill(6)}"

    pid = choose_patient_id()
    p = PatientRegistry[pid]

    hosp_id = random.choice(HospitalIDs)
    hosp = HospitalByID[hosp_id]

    visit_dt, is_admit, admit_dt, discharge_dt = choose_visit_dates()

    row = {
        "RecordID": record_id,
        "VisitDate": visit_dt.date().isoformat(),
        "IsAdmission": int(is_admit),
        "AdmissionDate": admit_dt.date().isoformat() if admit_dt else "",
        "DischargeDate": discharge_dt.date().isoformat() if discharge_dt else "",
        "HospitalID": hosp["HospitalID"],
        "HospitalName": hosp["HospitalName"],
        "HospitalLocation": hosp["HospitalLocation"],
        "HospitalAddress": hosp["HospitalAddress"],
        "GovPrivate": hosp["GovPrivate"],
        "HospitalSpecialization": hosp["HospitalSpecialization"],
        "PatientID": pid,
        "PatientFirstName": p["FirstName"],
        "PatientLastName": p["LastName"],
        "PatientFullName": p["FullName"],
        "PatientGenderID": p["Gender"][0],
        "PatientGender": p["Gender"][1],
        "PatientDOB": p["DOB"].isoformat(),
        "PatientStateID": p["State"][0],
        "PatientState": p["State"][1],
        "PatientCityID": p["City"][0],
        "PatientCity": p["City"][1],
        "PatientPhone": p["Phone"],
        "PatientEmail": p["Email"],
        "PatientBloodGroup": p["BloodGroup"]
    }

    rows.append(row)

# -----------------------------
# Write CSV
# -----------------------------
OUTPUT_FILE = "healthcare_dataset.csv"
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ Wrote {len(rows)} rows to {OUTPUT_FILE}")
