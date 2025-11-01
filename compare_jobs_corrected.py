#!/usr/bin/env python3
import csv

# Load corrected database data
db_jobs = {}
with open('october_all_jobs.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        job_id = row['job_id']
        db_jobs[job_id] = {
            'revenue': float(row['revenue']),
            'labor': float(row['labor']),
            'materials': float(row['materials']),
            'business_unit': row['business_unit']
        }

print(f'Database jobs loaded: {len(db_jobs)}')

# Now run the original comparison
exec(open('compare_jobs.py').read())