"""
Generate sample Wisconsin Provider Medicaid PSV file for testing
Run this if you don't have the real data file yet.

Usage:
    python generate_sample_data.py

Output:
    C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv
"""

import os
from datetime import datetime, timedelta
import random

# Sample data templates
PROVIDER_NAMES = [
    "ABC Medical Center", "XYZ Health Clinic", "Madison Regional Hospital",
    "Milwaukee Family Practice", "Green Bay Surgery Center",
    "Racine Urgent Care", "Kenosha Pediatrics", "Appleton Orthopedics"
]

CITIES = [
    "Madison", "Milwaukee", "Green Bay", "Kenosha", "Racine",
    "Appleton", "Waukesha", "Eau Claire", "Oshkosh", "Janesville"
]

SPECIALTIES = [
    ("001", "Family Practice"),
    ("002", "Internal Medicine"),
    ("003", "Pediatrics"),
    ("004", "Surgery"),
    ("005", "Orthopedics"),
    ("006", "Cardiology"),
    ("007", "Neurology"),
    ("008", "Psychiatry")
]

def generate_sample_file(output_path, num_providers=100):
    """Generate a sample PSV file with Wisconsin Provider Medicaid data"""
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    records = []
    record_count = 0
    
    # Type 00: File Information (Header)
    file_date = datetime.now().strftime("%Y-%m-%d")
    
    # Generate provider records
    for i in range(num_providers):
        provider_id = f"PROV{str(i+1).zfill(6)}"
        
        # Type 01: Provider
        provider_name = random.choice(PROVIDER_NAMES)
        dba_name = provider_name.replace("Center", "Clinic")
        status = random.choice(['A', 'A', 'A', 'I'])  # Mostly active
        eff_date = (datetime.now() - timedelta(days=random.randint(30, 1095))).strftime("%Y-%m-%d")
        term_date = "" if status == 'A' else (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d")
        prov_type = random.choice(['HOSP', 'CLINIC', 'PHYS', 'LTAC'])
        medicaid_num = f"MED{str(i+1).zfill(6)}"
        enroll_status = "Active" if status == 'A' else "Inactive"
        
        provider_record = f"01|{provider_id}|{provider_name}|{dba_name}|{status}|{eff_date}|{term_date}|{prov_type}|{medicaid_num}|{enroll_status}"
        records.append(provider_record)
        record_count += 1
        
        # Type 02: Address (Mailing)
        street = f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Maple', 'Cedar', 'Park'])} St"
        suite = f"Suite {random.randint(100, 999)}" if random.random() > 0.5 else ""
        city = random.choice(CITIES)
        zip_code = f"5{random.randint(3000, 3999)}"
        county = city + " County"
        
        address_record = f"02|{provider_id}|MAILING|{street}|{suite}|{city}|WI|{zip_code}|{county}|US|{eff_date}|"
        records.append(address_record)
        record_count += 1
        
        # Type 03: Tax ID (for some providers)
        if random.random() > 0.3:
            tax_id = f"{random.randint(10, 99)}-{random.randint(1000000, 9999999)}"
            tax_type = "EIN"
            tax_record = f"03|{provider_id}|{tax_id}|{tax_type}"
            records.append(tax_record)
            record_count += 1
        
        # Type 05: Specialty (1-2 per provider)
        num_specialties = random.randint(1, 2)
        for j in range(num_specialties):
            spec_code, spec_desc = random.choice(SPECIALTIES)
            primary = "Y" if j == 0 else "N"
            spec_record = f"05|{provider_id}|{prov_type}|{spec_code}|{spec_desc}|{primary}"
            records.append(spec_record)
            record_count += 1
        
        # Type 11: NPI (for most providers)
        if random.random() > 0.2:
            npi = f"{random.randint(1000000000, 1999999999)}"
            npi_type = random.choice(['1', '2'])
            npi_record = f"11|{provider_id}|{npi}|{npi_type}|{eff_date}"
            records.append(npi_record)
            record_count += 1
    
    # Add header at the beginning
    header = f"00|WI_PROV_FILE_EXTRACT.psv|{file_date}|{record_count}"
    records.insert(0, header)
    
    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(records))
    
    print(f"✅ Generated {record_count + 1} records (including header)")
    print(f"📁 File created: {output_path}")
    print(f"📊 Providers: {num_providers}")
    print(f"📝 Total records: {len(records)}")
    print("\nRecord type breakdown:")
    print(f"  Type 00 (Header): 1")
    print(f"  Type 01 (Provider): {num_providers}")
    print(f"  Type 02 (Address): {num_providers}")
    print(f"  Type 03 (Tax): {sum(1 for r in records if r.startswith('03'))}")
    print(f"  Type 05 (Specialty): {sum(1 for r in records if r.startswith('05'))}")
    print(f"  Type 11 (NPI): {sum(1 for r in records if r.startswith('11'))}")
    
    return output_path

if __name__ == "__main__":
    # Default output path for Windows
    output_path = r"C:\data\wi_medicaid\WI_PROV_FILE_EXTRACT.psv"
    
    print("=" * 60)
    print("Wisconsin Provider Medicaid - Sample Data Generator")
    print("=" * 60)
    print()
    
    # Check if file exists
    if os.path.exists(output_path):
        response = input(f"⚠️  File exists: {output_path}\nOverwrite? (y/n): ")
        if response.lower() != 'y':
            print("❌ Cancelled")
            exit(0)
    
    # Generate file
    try:
        generate_sample_file(output_path, num_providers=100)
        print("\n✅ Success! You can now run: dbt run")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure you have write permissions to C:\\data\\wi_medicaid\\")
        print("You may need to create the directory first:")
        print("  mkdir C:\\data\\wi_medicaid")
