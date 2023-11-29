import pandas as pd
import boto3
import hashlib
import random
from io import StringIO

def read_aws_credentials(file_path='secretkey.txt'):
    with open(file_path, 'r') as file:
        return file.read()

def retrieve_data_from_s3(bucket_name, file_name, aws_access_key_id, aws_secret_access_key):
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    
    s3 = session.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    content = response['Body'].read().decode('utf-8')
    
    return pd.read_csv(StringIO(content))

def upload_data_to_s3(bucket_name, file_name, aws_access_key_id, aws_secret_access_key,data):
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    
    s3 = session.client('s3')

    csv_data = data.to_csv(index=False)
    s3.put_object(Body=csv_data, Bucket=bucket_name, Key=file_name)

def check_k_anonymity(data, k_value, quasi_identifier_attributes):
    # Group data by quasi-identifier attributes
    grouped_data = data.groupby(quasi_identifier_attributes).size().reset_index(name='count')
    
    # Initialize a variable to track whether k-anonymity is satisfied for all groups
    k_anonymity_satisfied = True

    # Iterate through groups
    for index, record in data.iterrows():
        quasi_identifier_values = [record[attr] for attr in quasi_identifier_attributes]
        
        # Find the count for the current quasi-identifier values
        count = grouped_data[grouped_data[quasi_identifier_attributes].eq(quasi_identifier_values).all(axis=1)]['count'].values[0]
        
        if count < k_value:
            k_anonymity_satisfied = False
            print(f"K-anonymity not satisfied for group {quasi_identifier_values}. Count: {count}")

    return k_anonymity_satisfied

def check_l_diversity(data, quasi_identifier_attributes, columns_to_count,l_value):
    # Group data by quasi-identifier attributes
    grouped_data = data.groupby(quasi_identifier_attributes)

    # Initialize a variable to track whether l-diversity is satisfied for all groups
    l_diversity_satisfied = True

    # Iterate through groups
    for group_name, group_data in grouped_data:
        # Count unique records for specified columns within each group
        unique_counts = group_data[columns_to_count].drop_duplicates().shape[0]

        # Check if the number of unique records is less than l_value
        if unique_counts < l_value:  
            l_diversity_satisfied = False
            print(f"L-diversity not satisfied for group {group_name}. Unique counts: {unique_counts}")

    return l_diversity_satisfied


def calculate_t_closeness(data, quasi_identifier_attributes, sensitive_attribute, t_threshold):
    # Calculate the overall distribution of the sensitive attribute
    overall_distribution = data[sensitive_attribute].value_counts(normalize=True)

    # Group the data by quasi-identifier attributes
    grouped_data = data.groupby(quasi_identifier_attributes)

    # Initialize a variable to track whether t-closeness is satisfied for all groups
    t_closeness_satisfied = True

    # Iterate through groups
    for group_name, group_data in grouped_data:
        # Calculate the distribution of the sensitive attribute within the group
        group_distribution = group_data[sensitive_attribute].value_counts(normalize=True)

        # Calculate the difference between group and overall distributions
        difference = sum(abs(group_distribution - overall_distribution))

        # Check if the difference is less than the threshold 't'
        if difference > t_threshold:
            t_closeness_satisfied = False
            print(f"T-closeness not satisfied for group {group_name}. Difference: {difference:.4f}")

    return t_closeness_satisfied

def generalize_patient_id(patient_id):
    # Generalize patient ID by grouping every 100 patient IDs
    return f'Group_{int(patient_id[1:]) // 100}'

def pseudonymize_name(name):
    # Pseudonymize the name using MD5 hashing
    return f'Pseudo_{hashlib.md5((name+str(random.randint(1, 1000))).encode()).hexdigest()}'

def generalize_date_of_birth(dob):
    # Generalize date of birth into age groups
    birth_year = int(dob.split('-')[0])
    age_group = (2023 - birth_year) // 10
    return f'Age Group_{age_group}0s'

def anonymize_gender(gender):
    # Map original gender to anonymized groups
    gender_mapping = {
        'Male': 'Group-0',
        'Female': 'Group-1',
    }

    # Return the anonymized group for the given gender
    return gender_mapping.get(gender, 'Group_Other')

def redact_ssn(ssn):
    # Redact the Social Security Number except for the last four digits
    return 'xxx-xx-' + ssn[-4:]

def anonymize_street_address(address):
    # Split the address into components
    parts = address.split()

    # Check if the first part is a numeric value (street number)
    if parts and parts[0].isdigit():
        # Remove the numeric part
        parts = parts[1:]

    # Check if the last part is a numeric value
    if parts and parts[-1].isdigit():
        # Remove the numeric part from the end
        parts = parts[:-1]

    # Join the remaining components to form the anonymized address
    return ' '.join(parts)

def generalize_location(row):
    # Generalize location by combining city and state information
    return f'City_{row["City"]}, State_{row["State"]}'

def suppress_category(category):
    # Suppress or anonymize a category
    return 'Suppressed'

def anonymize_phone_number(phone_number):
    # Anonymize phone number by keeping only the area code and last four digits
    parts = phone_number.split('-')
    area_code = parts[0]
    return f'{area_code}-xxx-xxxx'

def anonymize_medication_mapping(medication):
    # Anonymize medication names using predefined mappings
    medication_mappings = {
    'Insulin': 'Insulin',
    'Metformin': 'Insulin',
    'Gliclazide': 'Insulin',
    'Lisinopril': 'Blood Pressure Medication',
    'Amlodipine': 'Blood Pressure Medication',
    'Losartan': 'Blood Pressure Medication',
    'Albuterol': 'Inhaler',
    'Fluticasone': 'Inhaler',
    'Montelukast': 'Inhaler',
    'Sumatriptan': 'Migraine Medication',
    'Propranolol': 'Migraine Medication',
    'Topiramate': 'Migraine Medication',
    'Ibuprofen': 'Pain Relief',
    'Naproxen': 'Pain Relief',
    'Celecoxib': 'Pain Relief',
    'Loratadine': 'Allergy Medication',
    'Cetirizine': 'Allergy Medication',
    'Fexofenadine': 'Allergy Medication',
    'Sertraline': 'Antidepressant',
    'Fluoxetine': 'Antidepressant',
    'Escitalopram': 'Antidepressant',
    'Zolpidem': 'Sleep Aid',
    'Trazodone': 'Sleep Aid',
    'Doxepin': 'Sleep Aid',
    'Omeprazole': 'Digestive Medication',
    'Ranitidine': 'Digestive Medication',
    'Esomeprazole': 'Digestive Medication',
    'Atorvastatin': 'Cholesterol Medication',
    'Simvastatin': 'Cholesterol Medication',
    'Rosuvastatin': 'Cholesterol Medication',
    'Dextromethorphan': 'Cough Medication',
    'Guaifenesin': 'Cough Medication',
    'Codeine': 'Cough Medication',
    'Oseltamivir': 'Flu Medication',
    'Zanamivir': 'Flu Medication',
    'Peramivir': 'Flu Medication'
    }
    return medication_mappings.get(medication, medication)

if __name__ == "__main__":
    aws_access_key_id = 'AKIA5GZTC2RVMTZHGFLX'
    aws_secret_access_key = read_aws_credentials()
    
    bucket_name = 'anonymization-project'
    file_name = 'HIPAA Data.csv'
    
    data = retrieve_data_from_s3(bucket_name, file_name, aws_access_key_id, aws_secret_access_key)
    
    quasi_identifier_attributes = ['Date of Birth', 'Street Address', 'City', 'State', 'Zip Code']
    confidential_attributes = ['Medical Condition', 'Medication']
    
    # Set k-anonymity threshold
    k_value = 7
    k_anonymity_satisfied = check_k_anonymity(data, k_value, quasi_identifier_attributes)
    print("K-Anonymity is satisfied:", k_anonymity_satisfied)

    # Set l-diversity threshold
    l_value = 4
    l_diversity_satisfied = check_l_diversity(data, quasi_identifier_attributes, confidential_attributes,l_value)
    print("L-Diversity is satisfied:", l_diversity_satisfied)

    # Set t-closeness threshold
    t_threshold = 0.4
    t_closeness_satisfied = calculate_t_closeness(data, quasi_identifier_attributes, confidential_attributes[0], t_threshold)
    print("T-Closeness is satisfied:", t_closeness_satisfied)

    # Final decision based on k, l, and t
    if k_anonymity_satisfied and l_diversity_satisfied and t_closeness_satisfied:
        print("The dataset meets K-Anonymity, L-Diversity, and T-Closeness criteria.")
    else:
        print("The dataset does not meet one or more of the privacy criteria.")

    # Apply anonymization techniques to the DataFrame

    # Generalize Patient ID
    data['Patient ID'] = data['Patient ID'].apply(generalize_patient_id)

    # Pseudonymize Last Name
    data['Last Name'] = data['Last Name'].apply(pseudonymize_name)

    # Generalize Date of Birth
    data['Date of Birth'] = data['Date of Birth'].apply(generalize_date_of_birth)

    # Redact SSN
    data['SSN'] = data['SSN'].apply(redact_ssn)

    # Generalizing Gender Data
    data['Gender'] = data['Gender'].apply(anonymize_gender)

    # Removing Specific numerical information
    data['Address'] = data['Street Address'].apply(anonymize_street_address)

    # Generalize location
    data['Location'] = data.apply(generalize_location, axis=1)

    # Suppress ZIP Code
    data['Zip Code'] = data['Zip Code'].apply(suppress_category)

    # Anonymize Medication
    data['Medication'] = data['Medication'].map(anonymize_medication_mapping)

    # Anonymize Phone Number
    data['Phone Number'] = data['Phone Number'].apply(anonymize_phone_number)

    # Drop original location columns
    data = data.drop(['Street Address','City', 'State'], axis=1)

    file_name = 'Anonymized Patient Data.csv'

    upload_data_to_s3(bucket_name, file_name, aws_access_key_id, aws_secret_access_key,data)

    print("Anonymized Patient Data is uploaded to s3 bucket successfully")