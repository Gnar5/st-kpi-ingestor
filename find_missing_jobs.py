#!/usr/bin/env python3
import csv

# Load ServiceTitan job IDs
st_jobs = set()
with open('st_job_ids.txt', 'r') as f:
    for line in f:
        job_id = line.strip()
        if job_id:
            st_jobs.add(job_id)

print(f"ServiceTitan jobs: {len(st_jobs)}")

# Load our database job IDs
our_jobs = set()
with open('our_job_ids.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        job_id = row['job_id']
        if job_id:
            our_jobs.add(job_id)

print(f"Our BigQuery jobs: {len(our_jobs)}")

# Find missing jobs
missing_from_bq = st_jobs - our_jobs
extra_in_bq = our_jobs - st_jobs

print(f"\n=== MISSING FROM BIGQUERY ===")
print(f"Count: {len(missing_from_bq)}")
if missing_from_bq:
    print("Missing Job IDs:")
    for job_id in sorted(missing_from_bq)[:50]:  # Show first 50
        print(f"  {job_id}")

print(f"\n=== EXTRA IN BIGQUERY (not in ST export) ===")
print(f"Count: {len(extra_in_bq)}")
if extra_in_bq:
    print("Extra Job IDs:")
    for job_id in sorted(extra_in_bq)[:50]:  # Show first 50
        print(f"  {job_id}")

# Save missing jobs to file for investigation
with open('missing_jobs_from_bq.txt', 'w') as f:
    for job_id in sorted(missing_from_bq):
        f.write(job_id + '\n')

print(f"\nMissing jobs saved to missing_jobs_from_bq.txt")