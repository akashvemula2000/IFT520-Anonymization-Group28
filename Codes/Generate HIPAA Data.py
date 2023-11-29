from faker import Faker
import random
from datetime import datetime, timedelta
import pandas as pd
import boto3
import re

fake = Faker('en_US')

# Read AWS credentials from a file (secretkey.txt)
with open('secretkey.txt', 'r') as file:
    secretkey = file.read()

# Create an AWS session
session = boto3.Session(
    aws_access_key_id='AKIA5GZTC2RVMTZHGFLX',
    aws_secret_access_key=secretkey,
)

# Create an S3 client
s3 = session.client('s3')

# Define the S3 bucket name
bucket_name = 'anonymization-project'

def generate_phone_number():
    while True:
        raw_phone_number = fake.phone_number()
        # Use a regular expression to match US phone number format
        match = re.match(r'\+(\d{1,2})?[-.\s]*\(?(\d{3})\)?[-.\s]*(\d{3})[-.\s]*(\d{4})', raw_phone_number)
        if match:
            formatted_phone_number = "{}-{}-{}".format(match.group(2), match.group(3), match.group(4))
            return formatted_phone_number

def generate_patient_data(num_records):
    data = []

    # Mapping of medical conditions to medications
    medical_condition_mapping = {
    'Diabetes': ['Insulin', 'Metformin', 'Gliclazide'],
    'Hypertension': ['Lisinopril', 'Amlodipine', 'Losartan'],
    'Asthma': ['Albuterol', 'Fluticasone', 'Montelukast'],
    'Migraine': ['Sumatriptan', 'Propranolol', 'Topiramate'],
    'Arthritis': ['Ibuprofen', 'Naproxen', 'Celecoxib'],
    'Allergies': ['Loratadine', 'Cetirizine', 'Fexofenadine'],
    'Depression': ['Sertraline', 'Fluoxetine', 'Escitalopram'],
    'Insomnia': ['Zolpidem', 'Trazodone', 'Doxepin'],
    'Acid Reflux': ['Omeprazole', 'Ranitidine', 'Esomeprazole'],
    'High Cholesterol': ['Atorvastatin', 'Simvastatin', 'Rosuvastatin'],
    'Cough': ['Dextromethorphan', 'Guaifenesin', 'Codeine'],
    'Flu': ['Oseltamivir', 'Zanamivir', 'Peramivir']
    }

    for _ in range(num_records//7):
        date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=80)
        ssn = fake.ssn()
        street_address = fake.street_address()
        city = fake.city()
        state = fake.state_abbr()
        zip_code = fake.zipcode()

        for _ in range(7):
            data.append({
                'Patient ID': "P" + str(fake.unique.random_number(6)),
                'First Name': fake.first_name(),
                'Last Name': fake.last_name(),
                'Date of Birth': date_of_birth,
                'Gender': fake.random_element(elements=['Male', 'Female']),
                'SSN': ssn,
                'Phone Number': generate_phone_number(),
                'Medical Condition': fake.random_element(elements=list(medical_condition_mapping.keys())),
                'Medication': random.choice(medical_condition_mapping[fake.random_element(elements=list(medical_condition_mapping.keys()))]),
                'Street Address': street_address,
                'City': city,
                'State': state,
                'Zip Code': zip_code
            })

    random.shuffle(data)
    return data

def s3_upload(data):
    csv_data = data.to_csv(index=False)
    s3.put_object(Body=csv_data, Bucket=bucket_name, Key='HIPAA Data.csv')

if __name__ == "__main__":
    num_records = 3500   
    
    # Generate patient data and convert it to a DataFrame
    patient_data = generate_patient_data(num_records)


    df = pd.DataFrame(patient_data)
    
    # Upload the DataFrame to S3
    s3_upload(df)

    print("HIPAA Type Data is Generated and uploaded to s3 bucket successfully")
