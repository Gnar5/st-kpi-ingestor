#!/usr/bin/env python3
import csv
import json
from decimal import Decimal

def parse_currency(value):
    """Convert currency string to float"""
    if not value or value == '':
        return 0.0
    # Remove $ and commas
    cleaned = value.replace('$', '').replace(',', '')
    try:
        return float(cleaned)
    except:
        return 0.0

def load_servicetitan_data(filename):
    """Load ServiceTitan export data"""
    jobs = {}
    current_bu = None

    # Parse the pasted ServiceTitan data
    st_data = """
389324885,2139.46,0.00,2000.00,1611.33,Andy's Painting-Production
389438781,0.00,0.00,100.00,49.97,Andy's Painting-Production
389618389,0.00,73.33,0.00,0.00,Andy's Painting-Production
388742795,619.58,85.00,200.00,269.63,Andy's Painting-Production
389126602,1148.80,0.00,550.00,362.28,Andy's Painting-Production
389182944,1420.00,0.00,800.00,183.19,Andy's Painting-Production
389209474,2820.00,0.00,1012.00,593.41,Andy's Painting-Production
389478663,4496.00,0.00,1785.00,603.45,Andy's Painting-Production
389126650,2096.00,200.33,599.67,224.55,Andy's Painting-Production
389284427,2925.00,0.00,877.00,478.50,Andy's Painting-Production
388899959,5060.75,0.00,1253.00,1069.33,Andy's Painting-Production
389186404,650.00,0.00,195.00,95.37,Andy's Painting-Production
389323999,480.00,0.00,200.00,0.50,Andy's Painting-Production
397652196,854.00,141.67,158.33,49.80,Andy's Painting-Production
388524852,4030.76,0.00,1200.00,450.38,Andy's Painting-Production
388525076,3000.00,0.00,900.00,239.75,Andy's Painting-Production
388981346,1533.66,0.00,50.00,441.28,Andy's Painting-Production
397576600,0.00,0.00,0.00,167.45,Andy's Painting-Production
397653981,0.00,0.00,200.00,0.00,Andy's Painting-Production
397692507,0.00,158.00,0.00,45.69,Andy's Painting-Production
397655265,0.00,407.00,0.00,87.14,Andy's Painting-Production
397686112,200.00,0.00,200.00,95.91,Commercial-AZ-Production
389653357,2215.00,0.00,2000.00,1206.10,Commercial-AZ-Production
388934314,10538.80,628.66,2686.34,7384.13,Commercial-AZ-Production
397674675,0.00,133.33,142.97,0.00,Commercial-AZ-Production
397671692,0.00,0.00,210.00,88.58,Commercial-AZ-Production
397669665,0.00,624.59,1135.41,176.27,Commercial-AZ-Production
397663774,489.00,287.08,147.50,0.00,Commercial-AZ-Production
388940524,10919.25,3486.67,1738.00,3026.74,Commercial-AZ-Production
387563401,10429.02,2438.55,1702.45,3668.23,Commercial-AZ-Production
389449179,3030.00,393.66,820.67,352.63,Commercial-AZ-Production
388942963,19478.68,3272.53,2127.47,4097.11,Commercial-AZ-Production
388743945,2444.98,0.00,600.00,289.07,Commercial-AZ-Production
389664342,500.00,0.00,0.00,0.00,Commercial-AZ-Production
397656424,1049.62,696.66,0.00,130.47,Guaranteed Painting-Production
389635183,833.00,0.00,600.00,10.00,Guaranteed Painting-Production
388972633,1190.00,704.00,0.00,98.66,Guaranteed Painting-Production
397656404,20323.52,0.00,9411.76,2524.88,Guaranteed Painting-Production
387377241,5166.89,0.00,2028.04,596.91,Guaranteed Painting-Production
388744199,3880.00,0.00,1604.51,335.53,Guaranteed Painting-Production
397951702,800.00,381.33,0.00,0.00,Guaranteed Painting-Production
389126476,5000.00,0.00,2500.00,2511.31,Nevada-Production
389148898,4240.00,370.00,750.00,1588.65,Nevada-Production
389022627,3320.00,420.00,680.00,886.24,Nevada-Production
389294575,11920.00,1505.66,1500.00,3583.50,Nevada-Production
389433089,1190.00,0.00,600.00,40.77,Nevada-Production
397690209,1190.00,0.00,500.00,53.50,Nevada-Production
389205021,900.00,0.00,400.00,0.00,Nevada-Production
389024855,1280.00,0.00,400.00,126.51,Nevada-Production
387052938,5200.00,0.00,0.00,0.00,Nevada-Production
397596121,2422.50,2252.10,0.00,875.46,Phoenix-Production
367637561,0.00,438.00,0.00,203.43,Phoenix-Production
366611946,0.00,0.00,1125.00,0.00,Phoenix-Production
389359078,0.00,0.00,0.00,44.76,Phoenix-Production
389266117,0.00,228.50,0.00,117.26,Phoenix-Production
397579507,0.00,0.00,0.00,193.80,Phoenix-Production
397675313,0.00,694.50,427.76,320.85,Phoenix-Production
397802313,0.00,160.30,0.00,741.79,Phoenix-Production
397657967,1200.00,0.00,720.00,463.27,Phoenix-Production
397597603,5000.00,0.00,4500.00,317.91,Phoenix-Production
389215111,1537.50,0.00,600.00,660.55,Phoenix-Production
389205072,640.00,0.00,500.00,20.21,Phoenix-Production
387264608,3593.28,0.00,1400.00,1337.14,Phoenix-Production
365622028,10900.00,0.00,3860.00,3764.74,Phoenix-Production
397672182,400.35,0.00,150.00,114.55,Phoenix-Production
366912331,4950.00,0.00,2000.00,1213.35,Phoenix-Production
397706800,3612.50,0.00,1570.00,767.00,Phoenix-Production
397653732,500.00,0.00,250.00,72.19,Phoenix-Production
397594304,3850.00,0.00,1250.00,1229.56,Phoenix-Production
389145116,8880.00,0.00,2700.00,2857.20,Phoenix-Production
397658510,1500.00,0.00,840.00,62.68,Phoenix-Production
387625123,3350.00,0.00,1298.20,711.80,Phoenix-Production
388974729,1241.17,307.65,0.00,425.01,Phoenix-Production
387680999,4970.40,0.00,1400.00,1467.62,Phoenix-Production
389336810,500.00,0.00,200.00,83.25,Phoenix-Production
388939000,7093.25,519.00,1981.00,1499.43,Phoenix-Production
389437660,900.00,0.00,300.00,206.20,Phoenix-Production
389617947,3700.05,0.00,1150.00,902.54,Phoenix-Production
387244959,3780.00,192.82,857.18,1002.31,Phoenix-Production
388510634,5885.40,533.34,866.66,1775.09,Phoenix-Production
388413301,1920.00,0.00,700.00,333.51,Phoenix-Production
389612440,800.70,0.00,300.00,127.35,Phoenix-Production
361712253,4312.00,0.00,1200.00,1078.35,Phoenix-Production
389083995,3561.50,0.00,1000.00,854.61,Phoenix-Production
388881200,8384.00,0.00,2220.90,2085.10,Phoenix-Production
389664708,3500.00,0.00,1500.00,292.59,Phoenix-Production
367596089,8530.00,0.00,1999.51,2265.49,Phoenix-Production
389083232,3485.00,0.00,985.38,757.12,Phoenix-Production
397654361,500.00,0.00,250.00,0.00,Phoenix-Production
388675028,3985.65,0.00,1547.50,445.32,Phoenix-Production
387744395,5647.40,0.00,1977.43,876.79,Phoenix-Production
388656562,4326.50,0.00,1515.94,628.16,Phoenix-Production
389003965,2685.75,0.00,900.00,418.10,Phoenix-Production
389007816,4900.00,0.00,1300.00,1072.59,Phoenix-Production
389184844,3400.00,409.88,610.12,619.95,Phoenix-Production
388979117,785.00,220.00,0.00,157.60,Phoenix-Production
387826576,4269.15,0.00,1800.00,246.10,Phoenix-Production
388015394,3480.00,0.00,1101.50,725.74,Phoenix-Production
397830495,4400.00,468.65,1100.00,489.57,Phoenix-Production
388978707,1650.00,12.25,350.00,406.41,Phoenix-Production
388067731,6365.00,0.00,2400.00,517.68,Phoenix-Production
367228489,2310.20,0.00,200.00,854.26,Phoenix-Production
389182317,2985.60,0.00,800.00,544.44,Phoenix-Production
388072660,5985.00,799.05,1100.95,761.07,Phoenix-Production
397655909,5000.00,0.00,1740.00,444.03,Phoenix-Production
387848312,3580.00,0.00,1100.00,386.30,Phoenix-Production
389438180,5360.00,0.00,1200.00,898.24,Phoenix-Production
388987517,3500.00,0.00,500.00,810.23,Phoenix-Production
388657413,4985.00,340.00,660.00,868.35,Phoenix-Production
389308932,7743.50,0.00,1140.00,1673.62,Phoenix-Production
397667192,2500.00,0.00,460.00,542.90,Phoenix-Production
397596564,2450.00,0.00,650.00,127.75,Phoenix-Production
397654339,1000.00,0.00,300.00,0.00,Phoenix-Production
397677704,1200.00,0.00,320.00,0.00,Phoenix-Production
389080703,8000.00,510.37,1200.00,217.00,Phoenix-Production
387922418,250.00,0.00,0.00,0.00,Phoenix-Production
364255346,400.00,0.00,0.00,0.00,Phoenix-Production
389664675,2600.29,754.78,825.31,1477.33,Tucson-Production
388751942,0.00,0.00,251.00,0.00,Tucson-Production
397792892,0.00,223.50,0.00,0.00,Tucson-Production
397747162,0.00,0.00,65.00,0.00,Tucson-Production
397644200,0.00,222.92,0.00,130.44,Tucson-Production
389591994,0.00,0.00,0.00,19.65,Tucson-Production
387850483,0.00,0.00,85.50,102.12,Tucson-Production
387979122,0.00,80.10,0.00,0.00,Tucson-Production
388781334,0.00,117.00,0.00,0.00,Tucson-Production
389653681,5000.00,594.71,905.29,2832.12,Tucson-Production
389666834,2359.61,677.10,888.50,229.64,Tucson-Production
388571483,2458.14,513.33,600.00,718.83,Tucson-Production
389646395,5000.00,719.59,960.00,1992.52,Tucson-Production
387843470,5091.89,404.72,1122.85,1673.39,Tucson-Production
397666477,1390.00,209.00,208.00,446.23,Tucson-Production
389568833,4957.61,667.22,824.92,1498.43,Tucson-Production
388788307,4185.88,418.20,837.56,1199.36,Tucson-Production
397768877,629.73,0.00,188.91,179.53,Tucson-Production
365350342,6070.04,391.80,1429.21,1370.76,Tucson-Production
389664616,500.00,0.00,150.00,112.19,Tucson-Production
365207798,585.00,69.68,105.82,127.55,Tucson-Production
387987187,3645.18,355.47,738.08,759.19,Tucson-Production
363024202,9000.00,0.00,2700.00,1709.61,Tucson-Production
397673826,1200.00,223.75,193.00,164.80,Tucson-Production
388492644,354.92,0.00,95.00,75.76,Tucson-Production
389499476,6215.97,470.25,1394.54,1104.39,Tucson-Production
397592957,7000.00,1869.50,1085.00,375.02,Tucson-Production
397644686,2208.42,364.09,431.78,214.20,Tucson-Production
389480902,824.82,72.08,175.36,102.86,Tucson-Production
397853120,150.00,0.00,60.00,0.00,Tucson-Production
397644337,1000.00,0.00,300.00,95.89,Tucson-Production
389442906,913.11,142.80,141.13,75.24,Tucson-Production
389643709,7161.78,498.00,1650.57,614.99,Tucson-Production
397556470,450.00,0.00,135.00,29.56,Tucson-Production
397703002,4000.00,405.17,794.83,232.24,Tucson-Production
389671462,2471.08,326.70,414.62,94.28,Tucson-Production
397555115,1052.14,182.08,133.56,0.25,Tucson-Production
389666802,668.40,94.97,105.55,0.10,Tucson-Production
397594433,5000.00,627.60,872.40,0.00,Tucson-Production
397643384,873.26,73.95,72.26,109.30,Tucson-Production
"""

    for line in st_data.strip().split('\n'):
        parts = line.split(',')
        if len(parts) == 6:
            job_id = parts[0]
            revenue = float(parts[1])
            labor_pay = float(parts[2])
            payroll_adj = float(parts[3])
            materials = float(parts[4])
            bu = parts[5]

            jobs[job_id] = {
                'revenue': revenue,
                'labor': labor_pay + payroll_adj,  # Combine labor pay and adjustments
                'materials': materials,
                'business_unit': bu
            }

    return jobs

def load_database_data(filename):
    """Load our database export data"""
    jobs = {}
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            job_id = row['job_id']
            jobs[job_id] = {
                'revenue': float(row['revenue']),
                'labor': float(row['labor']),
                'materials': float(row['materials']),
                'business_unit': row['business_unit']
            }
    return jobs

def compare_jobs(st_jobs, db_jobs):
    """Compare ServiceTitan and database jobs"""
    discrepancies = []

    # Find jobs in both datasets
    all_job_ids = set(st_jobs.keys()) | set(db_jobs.keys())

    for job_id in all_job_ids:
        st_job = st_jobs.get(job_id, {})
        db_job = db_jobs.get(job_id, {})

        if not st_job:
            # Job only in database
            if db_job['labor'] > 0 or db_job['materials'] > 0:
                discrepancies.append({
                    'job_id': job_id,
                    'business_unit': db_job['business_unit'],
                    'type': 'DB_ONLY',
                    'labor_diff': -db_job['labor'],
                    'materials_diff': -db_job['materials']
                })
        elif not db_job:
            # Job only in ServiceTitan
            if st_job['labor'] > 0 or st_job['materials'] > 0:
                discrepancies.append({
                    'job_id': job_id,
                    'business_unit': st_job['business_unit'],
                    'type': 'ST_ONLY',
                    'labor_diff': st_job['labor'],
                    'materials_diff': st_job['materials']
                })
        else:
            # Job in both - check for differences
            labor_diff = db_job['labor'] - st_job['labor']
            materials_diff = db_job['materials'] - st_job['materials']

            if abs(labor_diff) > 0.01 or abs(materials_diff) > 0.01:
                discrepancies.append({
                    'job_id': job_id,
                    'business_unit': st_job['business_unit'],
                    'type': 'DIFFERENT',
                    'st_labor': st_job['labor'],
                    'db_labor': db_job['labor'],
                    'labor_diff': labor_diff,
                    'st_materials': st_job['materials'],
                    'db_materials': db_job['materials'],
                    'materials_diff': materials_diff
                })

    return discrepancies

# Main execution
if __name__ == "__main__":
    print("Loading ServiceTitan data...")
    st_jobs = load_servicetitan_data('st_export.csv')

    print("Loading database data...")
    db_jobs = load_database_data('october_all_jobs.csv')

    print(f"\nServiceTitan jobs: {len(st_jobs)}")
    print(f"Database jobs: {len(db_jobs)}")

    print("\nComparing jobs...")
    discrepancies = compare_jobs(st_jobs, db_jobs)

    # Sort by total discrepancy
    discrepancies.sort(key=lambda x: abs(x['labor_diff']) + abs(x['materials_diff']), reverse=True)

    # Summary by business unit
    bu_summary = {}
    for disc in discrepancies:
        bu = disc['business_unit']
        if bu not in bu_summary:
            bu_summary[bu] = {'labor_diff': 0, 'materials_diff': 0, 'count': 0}
        bu_summary[bu]['labor_diff'] += disc['labor_diff']
        bu_summary[bu]['materials_diff'] += disc['materials_diff']
        bu_summary[bu]['count'] += 1

    print("\n=== SUMMARY BY BUSINESS UNIT ===")
    for bu, summary in sorted(bu_summary.items()):
        print(f"\n{bu}:")
        print(f"  Jobs with differences: {summary['count']}")
        print(f"  Total Labor Difference: ${summary['labor_diff']:,.2f}")
        print(f"  Total Materials Difference: ${summary['materials_diff']:,.2f}")

    print("\n=== TOP 20 JOBS WITH LARGEST DISCREPANCIES ===")
    for i, disc in enumerate(discrepancies[:20], 1):
        print(f"\n{i}. Job {disc['job_id']} ({disc['business_unit']}):")
        print(f"   Type: {disc['type']}")
        if disc['type'] == 'DIFFERENT':
            print(f"   Labor: ST=${disc['st_labor']:,.2f} vs DB=${disc['db_labor']:,.2f} (Diff: ${disc['labor_diff']:,.2f})")
            print(f"   Materials: ST=${disc['st_materials']:,.2f} vs DB=${disc['db_materials']:,.2f} (Diff: ${disc['materials_diff']:,.2f})")
        else:
            print(f"   Labor Difference: ${disc['labor_diff']:,.2f}")
            print(f"   Materials Difference: ${disc['materials_diff']:,.2f}")