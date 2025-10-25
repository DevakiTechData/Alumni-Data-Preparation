#!/usr/bin/env python3
# SLU Alumni Project — Synthetic Data Generator
# Generates 1,000 rows x 20 columns for each table and saves CSVs locally.
# Requirements: pandas, numpy, faker
#   pip install pandas numpy faker python-dateutil
# Run:
#   python slu_seed_generator.py

import uuid
from datetime import datetime, timedelta, date
import random
import pandas as pd
import numpy as np
from faker import Faker

SEED = 20251024
ROWS = 1000
fake = Faker()
Faker.seed(SEED)
random.seed(SEED)
np.random.seed(SEED)

# ---------- Helpers ----------
def uid():
    return str(uuid.uuid4())

def weighted_choice(pairs):
    total = sum(w for _, w in pairs)
    r = random.uniform(0, total)
    upto = 0
    for v, w in pairs:
        if upto + w >= r:
            return v
        upto += w
    return pairs[-1][0]

def rand_phone():
    return f"+1{random.randint(200,999)}{random.randint(200,999)}{random.randint(1000,9999)}"

def clean_email(s):
    return s.lower().replace(' ', '')

# ---------- 1) students_1 (20 cols) ----------
def make_students(n=ROWS):
    programs = [
        ("MS Information Systems", "School of Science & Engineering", "Data Analytics"),
        ("MS Computer Science", "School of Science & Engineering", "AI & ML"),
        ("MS Data Science", "School of Science & Engineering", "Big Data"),
        ("MS Software Engineering", "School of Science & Engineering", "Cloud Computing"),
        ("MS Business Analytics", "Chaifetz School of Business", "Predictive Modeling"),
        ("MBA", "Chaifetz School of Business", "Business Analytics"),
        ("MS Cybersecurity", "School of Science & Engineering", "Network Security"),
        ("MS Information Technology", "School of Science & Engineering", "Networking"),
    ]
    start_terms = ["Fall-2023","Spring-2024","Fall-2024","Spring-2025"]
    grad_terms  = ["Spring-2025","Fall-2025","Spring-2026","Fall-2026"]
    cities = [
        ("St. Louis","MO","USA"), ("Chicago","IL","USA"), ("Dallas","TX","USA"),
        ("Austin","TX","USA"), ("Seattle","WA","USA"), ("Boston","MA","USA"),
        ("New York","NY","USA"), ("Hyderabad",None,"India"), ("Bengaluru",None,"India")
    ]

    rows = []
    for i in range(1, n+1):
        first = fake.first_name()
        last  = fake.last_name()
        preferred = first if i % 4 != 0 else None

        prog, college, conc = random.choice(programs)
        st = random.choice(start_terms)
        gt = random.choice(grad_terms)
        gy = 2024 + (i % 3)

        visa = weighted_choice([("F1", 6), ("H1B",1), ("PR",1), ("Citizen",1), ("Other",1)])
        if visa == "F1":
            opt = weighted_choice([("None",1), ("OPT",2), ("STEM OPT",2)])
        else:
            opt = "None"

        city, state, country = random.choice(cities)
        slu_email = f"{first[0]}{last}.{i}@slu.edu".lower()
        personal  = f"{first}.{last}{i}@example.com".lower()

        rows.append({
            "student_id": uid(),
            "slu_banner_id": f"B{100000 + i:07d}"[0:8],
            "first_name": first,
            "last_name": last,
            "preferred_name": preferred,
            "slu_email": clean_email(slu_email),
            "personal_email": clean_email(personal),
            "phone_e164": rand_phone(),
            "program_name": prog,
            "college_name": college,
            "concentration": conc,
            "start_term": st,
            "grad_term": gt,
            "graduation_year": gy,
            "visa_status": visa,
            "opt_status": opt,
            "current_location_city": city,
            "current_location_state": state if state else "",
            "current_location_country": country,
            "linkedin_url": f"https://www.linkedin.com/in/slu-student-{i}",
        })
    return pd.DataFrame(rows)

# ---------- 2) employers_1 (20 cols) ----------
def make_employers(n=ROWS):
    industries = [
        ("Technology","Cloud/Software"),
        ("Consulting","IT Services"),
        ("Healthcare","Provider"),
        ("Finance","Banking"),
        ("Education","University"),
        ("Manufacturing","Industrial"),
        ("Retail","E-commerce"),
        ("Government","Public Sector")
    ]
    cities = [
        ("St. Louis","MO","USA"), ("Chicago","IL","USA"), ("Dallas","TX","USA"),
        ("Austin","TX","USA"), ("Seattle","WA","USA"), ("New York","NY","USA"),
        ("San Jose","CA","USA"), ("Hyderabad",None,"India")
    ]
    size_band = ["Small","Medium","Large","Enterprise"]
    partnership_type = ["None","Hiring","Academic","Mentorship","Sponsorship","Donor"]
    partnership_level = ["Bronze","Silver","Gold"]
    partnership_status = ["Active","Prospect","Inactive"]

    rows = []
    for i in range(1, n+1):
        name = f"Employer {i:04d}"
        ind, sub = random.choice(industries)
        city, state, country = random.choice(cities)
        is_faang = 1 if i % 200 == 0 else 0
        is_np = 1 if i % 10 in (3,7) else 0
        rows.append({
            "employer_id": uid(),
            "employer_name": name,
            "industry": ind,
            "sub_industry": sub,
            "company_size_band": random.choice(size_band),
            "hq_city": city,
            "hq_state": state if state else "",
            "hq_country": country,
            "website_url": f"https://www.example{i}.com",
            "linkedin_url": f"https://www.linkedin.com/company/example-{i}",
            "is_faang_company": is_faang,
            "is_non_profit": is_np,
            "slu_partnership_type": random.choice(partnership_type),
            "slu_partnership_level": random.choice(partnership_level),
            "slu_partnership_status": random.choice(partnership_status),
            "slu_partnership_start_date": (date.today() - timedelta(days=random.randint(30,900))).isoformat(),
            "primary_contact_name": fake.name(),
            "primary_contact_title": "Recruiter",
            "primary_contact_email": clean_email(f"contact{i}@example.com"),
            "primary_contact_phone": rand_phone()
        })
    return pd.DataFrame(rows)

# ---------- 3) jobs_1 (20 cols) ----------
def make_jobs(students_df, employers_df, n=ROWS):
    titles = ["Software Engineer","Data Analyst","Data Engineer","QA Engineer","Associate Consultant","Business Analyst","Security Analyst"]
    families = ["Engineering","Analytics","Quality","Consulting","Security"]
    levels = ["Intern","Junior","Mid","Senior"]
    jtypes = ["Full-time","Contract","Internship"]
    modes  = ["On-site","Hybrid","Remote"]
    channels = ["Career Fair","Referral","Handshake","LinkedIn","Direct Apply"]

    rows = []
    for i in range(1, n+1):
        stu = students_df.sample(1, random_state=SEED+i).iloc[0]
        emp = employers_df.sample(1, random_state=SEED*2+i).iloc[0]
        faang = emp["is_faang_company"] == 1
        nonp  = emp["is_non_profit"] == 1
        base = 100000 if faang else 60000 if nonp else 80000
        salary = base + random.randint(0, 60000 if faang else 30000 if nonp else 40000)
        start = date.today() - timedelta(days=random.randint(0,360))
        offer = start - timedelta(days=random.randint(1,40))
        rows.append({
            "job_id": uid(),
            "student_id": stu["student_id"],
            "employer_id": emp["employer_id"],
            "job_title": random.choice(titles),
            "job_family": random.choice(families),
            "job_level": random.choice(levels),
            "job_type": random.choice(jtypes),
            "employment_mode": random.choice(modes),
            "location_city": emp["hq_city"],
            "location_state": emp["hq_state"],
            "location_country": emp["hq_country"],
            "offer_date": offer.isoformat(),
            "offer_accept_date": (offer + timedelta(days=random.randint(0,7))).isoformat(),
            "start_date": start.isoformat(),
            "end_date": (start + timedelta(days=random.randint(90, 365))).isoformat() if random.random() < 0.2 else "",
            "salary_currency": "USD",
            "salary_base_annual": float(salary),
            "bonus_target_pct": round(random.random()*20, 2),
            "visa_type": stu["opt_status"] if stu["opt_status"] in ("OPT","STEM OPT") else "None",
            "source_channel": random.choice(channels),
            "created_at": datetime.utcnow().isoformat(timespec="seconds")
        })
    return pd.DataFrame(rows)

# ---------- 4) events_1 (20 cols) ----------
def make_events(n=ROWS):
    types = ["Career Fair","Webinar","Workshop","Meetup","Guest Lecture"]
    themes = ["AI & Data Analytics Careers","Cloud & DevOps Pathways","Cybersecurity Trends 2025","Product & Project Management","Consulting Case Prep","Resume & Interview Mastery","Networking Night","Employer Spotlight"]
    modes = ["In-person","Virtual","Hybrid"]
    tzs = ["America/Chicago","America/New_York","America/Los_Angeles"]
    venues = [
        ("Busch Student Center","St. Louis","MO","USA"),
        ("Chaifetz Arena","St. Louis","MO","USA"),
        ("Career Services Hall","Chicago","IL","USA"),
        ("Tech Innovation Lab","Austin","TX","USA"),
        ("Data Science Hub","Seattle","WA","USA"),
        ("Alumni Center","Boston","MA","USA"),
        ("Virtual Stage","Online","","USA"),
        ("Global Webinar Room","Online","","USA"),
    ]
    orgs = [("Career Services","careerservices@slu.edu"),
            ("Alumni Office","alumni@slu.edu"),
            ("School of Science & Engineering","sse-events@slu.edu"),
            ("Chaifetz School of Business","chaifetz-events@slu.edu"),
            ("Cybersecurity Center","cybercenter@slu.edu"),
            ("Data Science Institute","dsi@slu.edu")]

    rows = []
    base = datetime.combine(date.today(), datetime.min.time())
    for i in range(1, n+1):
        etype = types[i % len(types)]
        theme = themes[i % len(themes)]
        start = base + timedelta(days=random.randint(-180,179), hours=random.choice([10,14,18]))
        duration = random.randint(1,4)
        end = start + timedelta(hours=duration)
        tz = random.choice(tzs)
        venue, city, st, country = random.choice(venues)
        org_unit, org_email = random.choice(orgs)
        cap = { "Career Fair": random.randint(500,1200),
                "Webinar": random.randint(200,600),
                "Workshop": random.randint(30,150),
                "Meetup": random.randint(50,200),
                "Guest Lecture": random.randint(80,230) }[etype]
        regs = min(cap, 100 + random.randint(0, 600))
        atts = min(regs, 80 + random.randint(0, 400))
        fb = round(random.uniform(3.5,5.0), 2) if random.random() > 0.1 and atts > 0 else ""

        rows.append({
            "event_id": uid(),
            "event_name": f"{etype} {i:04d} • {theme}",
            "event_type": etype,
            "event_theme": theme,
            "event_description": f"Join us for {etype.lower()} on {theme.lower()}.",
            "start_datetime": start.isoformat(sep=' '),
            "end_datetime": end.isoformat(sep=' '),
            "timezone": tz,
            "delivery_mode": random.choice(modes),
            "location_venue": venue,
            "location_city": city,
            "location_state": st,
            "location_country": country,
            "organizer_unit": org_unit,
            "organizer_contact_email": org_email,
            "capacity": cap,
            "rsvp_required": 1 if etype in ("Webinar","Workshop","Guest Lecture") else random.choice([0,1]),
            "registrations_count": regs,
            "attendees_count": atts,
            "feedback_score_avg": fb
        })
    return pd.DataFrame(rows)

# ---------- 5) alumni_1 (20 cols) ----------
def make_alumni(students_df, jobs_df):
    jobs_df2 = jobs_df.copy()
    jobs_df2["start_date_dt"] = pd.to_datetime(jobs_df2["start_date"], errors="coerce" )
    latest = jobs_df2.sort_values(["student_id","start_date_dt"]).groupby("student_id").tail(1)

    rows = []
    for _, stu in students_df.iterrows():
        sid = stu["student_id"]
        lj = latest[latest["student_id"] == sid]
        employer_id = lj["employer_id"].iloc[0] if not lj.empty else ""
        current_title = lj["job_title"].iloc[0] if not lj.empty else ""
        start_dt = lj["start_date_dt"].iloc[0] if not lj.empty else pd.NaT
        years_exp = 0.0 if pd.isna(start_dt) else round(((pd.Timestamp.today() - start_dt).days)/365.0, 1)

        stage = "Early" if years_exp < 2 else "Mid" if years_exp < 5 else "Senior" if years_exp < 10 else "Lead"

        rows.append({
            "alumni_id": uid(),
            "student_id": sid,
            "current_employer_id": employer_id,
            "current_title": current_title,
            "years_experience": years_exp,
            "career_stage": stage,
            "skills_primary": random.choice(["Python, SQL, Statistics","Java, Algorithms, DS","Git, CI/CD, Microservices","Power BI, Excel, Storytelling","Network Sec, Splunk, IAM","ETL, Databases, Cloud"]),
            "skills_secondary": random.choice(["ML, DL, MLOps","AWS, Azure, GCP","SIEM, SOC","DAX, Modeling, Dashboards","Spark, Hadoop, PySpark","Communication, Teamwork"]),
            "certifications": random.choice(["AWS CCP","Azure Fundamentals","None","PMP","Security+"]),
            "achievements": random.choice(["Dean's List","Hackathon Winner","Published Paper","Volunteer Lead",""]),
            "mentoring_interest": random.choice([0,1]),
            "volunteering_interest": random.choice([0,1]),
            "is_active_member": random.choice([0,1]),
            "last_engagement_date": (date.today() - timedelta(days=random.randint(0,720))).isoformat() if random.random()<0.3 else "",
            "profile_visibility": random.choice(["Public","SLU-only","Private"]),
            "preferred_contact_method": random.choice(["Email","LinkedIn","Phone"]),
            "preferred_time_zone": random.choice(["America/Chicago","America/New_York","America/Los_Angeles"]),
            "portfolio_url": f"https://portfolio.example.com/{clean_email(stu['slu_email']).split('@')[0]}",
            "github_url": f"https://github.com/{stu['first_name'][0].lower()}{stu['last_name'].lower()}",
            "created_at": datetime.utcnow().isoformat(timespec="seconds")
        })
    df = pd.DataFrame(rows)
    return df.head(ROWS)

# ---------- 6) event_attendance_1 (20 cols) ----------
def make_event_attendance(events_df, students_df):
    rows = []
    per_event_cap = 300
    students = students_df["student_id"].tolist()
    for _, ev in events_df.iterrows():
        regs = min(ev["registrations_count"], per_event_cap, len(students))
        atts = min(ev["attendees_count"], regs)
        chosen = random.sample(students, int(regs)) if regs > 0 else []
        for j, sid in enumerate(chosen, start=1):
            attended = 1 if j <= atts else 0
            attended_at = (pd.to_datetime(ev["start_datetime"]) + timedelta(minutes=(j % 60))).isoformat(sep=' ') if attended else ""
            rows.append({
                "attendance_id": uid(),
                "event_id": ev["event_id"],
                "student_id": sid,
                "registration_status": "Registered",
                "registration_channel": random.choice(["Portal","Email","Onsite"]),
                "registered_at": (pd.to_datetime(ev["start_datetime"]) - timedelta(days=1)).isoformat(sep=' '),
                "attended": attended,
                "attended_at": attended_at,
                "check_in_method": random.choice(["QR","Manual","Import"]) if attended else "",
                "feedback_score": round(random.uniform(3.5,5.0),2) if attended and random.random()>0.2 else "",
                "feedback_comment": "",
                "certificate_issued": 0,
                "no_show_reason": "" if attended else random.choice(["Sick","Conflict",""]),
                "reminder_sent": random.choice([0,1]),
                "reminder_sent_at": (pd.to_datetime(ev["start_datetime"]) - timedelta(days=2)).isoformat(sep=' '),
                "created_by_user": "seed_bot",
                "created_at": datetime.utcnow().isoformat(sep=' ', timespec="seconds"),
                "updated_by_user": "seed_bot",
                "updated_at": "",
                "privacy_level": random.choice(["Public","Internal","Restricted"])
            })
    df = pd.DataFrame(rows)
    if len(df) < ROWS:
        pad = []
        for i in range(ROWS - len(df)):
            r = df.iloc[i % max(1,len(df))].copy()
            r["attendance_id"] = uid()
            pad.append(r)
        df = pd.concat([df, pd.DataFrame(pad)], ignore_index=True)
    return df.head(ROWS)

# ---------- 7) engagements_1 (20 cols) ----------
def make_engagements(alumni_df, employers_df, events_df):
    types = ["Mentorship","Sponsorship","Talk","Donation","Job Post","Volunteering","Workshop","Panel"]
    subtypes = {
        "Mentorship":"Career Mentoring","Sponsorship":"Event Sponsor","Talk":"Guest Talk",
        "Donation":"Alumni Donation","Job Post":"Full-time Role","Volunteering":"Community Service",
        "Workshop":"Hands-on Session","Panel":"Industry Panel"
    }
    channels = ["Email","LinkedIn","Handshake","In-person"]
    privs = ["Public","Internal","Restricted"]

    al_ids = alumni_df["alumni_id"].tolist()
    emp_ids = employers_df["employer_id"].tolist()
    ev_ids = events_df["event_id"].tolist()

    rows = []
    for i in range(1, ROWS+1):
        et = random.choice(types)
        hours = None
        if et in ("Mentorship","Talk","Volunteering","Workshop"):
            hours = round(random.uniform(1,6),2) if et!="Talk" else round(random.uniform(1,3),2)
        money = None
        if et in ("Sponsorship","Donation"):
            money = round(random.uniform(100,3000),2) if et=="Donation" else round(random.uniform(500,5000),2)

        has_event = (i % 5) in (0,1,2)
        has_emp   = (i % 2) == 0

        ev = random.choice(ev_ids) if has_event and ev_ids else ""
        emp = random.choice(emp_ids) if has_emp and emp_ids else ""

        ev_row = events_df[events_df["event_id"]==ev].iloc[0] if ev else None
        eng_date = (pd.to_datetime(ev_row["start_datetime"]).date() if ev_row is not None else (date.today() - timedelta(days=random.randint(0,720))))

        status = weighted_choice([("Completed",8),("Planned",1),("Cancelled",1)])
        sat = (round(random.uniform(3.5,5.0),2) if status=="Completed" else "")

        rows.append({
            "engagement_id": uid(),
            "alumni_id": random.choice(al_ids),
            "employer_id": emp,
            "event_id": ev,
            "engagement_type": et,
            "engagement_subtype": subtypes[et],
            "engagement_date": eng_date.isoformat(),
            "points_awarded": {"Mentorship":10,"Sponsorship":15,"Talk":8,"Donation":20,"Job Post":6,"Volunteering":12,"Workshop":10,"Panel":8}[et],
            "hours_contributed": hours if hours is not None else "",
            "monetary_value_usd": money if money is not None else "",
            "channel": random.choice(channels),
            "satisfaction_rating": sat,
            "remarks": "",
            "evidence_url": f"https://evidence.example.com/eng/{i}" if i % 3 == 0 else "",
            "created_by_user": "seed_bot",
            "created_at": datetime.utcnow().isoformat(sep=' ', timespec="seconds"),
            "status": status,
            "follow_up_required": 1 if i % 5 == 0 else 0,
            "follow_up_date": (eng_date + timedelta(days=7)).isoformat() if i % 5 == 0 else "",
            "privacy_level": random.choice(privs)
        })
    return pd.DataFrame(rows)

# ---------- 8) data_dictionary (single table for all) ----------
def make_data_dictionary(tables):
    rows = []
    for tname, df in tables.items():
        for col, dtype in df.dtypes.items():
            example = ""
            try:
                example = str(df[col].dropna().astype(str).head(1).values[0])
            except Exception:
                example = ""
            rows.append({
                "table_name": tname,
                "column_name": col,
                "data_type": str(dtype),
                "is_primary_key": 1 if col.endswith('_id') and col.count('_')==1 else 0,
                "is_foreign_key": 0,
                "foreign_table": "",
                "foreign_column": "",
                "is_nullable": 1 if df[col].isna().any() or (df[col].astype(str) == "").any() else 0,
                "allowed_values": "",
                "description": "",
                "example_value": example,
                "used_in_dashboard": ""
            })
    return pd.DataFrame(rows)[[
        "table_name","column_name","data_type","is_primary_key","is_foreign_key",
        "foreign_table","foreign_column","is_nullable","allowed_values","description",
        "example_value","used_in_dashboard"
    ]]

def main():
    students = make_students()
    employers = make_employers()
    jobs = make_jobs(students, employers)
    events = make_events()
    alumni = make_alumni(students, jobs)
    attendance = make_event_attendance(events, students)
    engagements = make_engagements(alumni, employers, events)

    students.to_csv("students_1.csv", index=False)
    employers.to_csv("employers_1.csv", index=False)
    jobs.to_csv("jobs_1.csv", index=False)
    events.to_csv("events_1.csv", index=False)
    alumni.to_csv("alumni_1.csv", index=False)
    attendance.to_csv("event_attendance_1.csv", index=False)
    engagements.to_csv("engagements_1.csv", index=False)

    dd = make_data_dictionary({
        "students_1": students,
        "employers_1": employers,
        "jobs_1": jobs,
        "events_1": events,
        "alumni_1": alumni,
        "event_attendance_1": attendance,
        "engagements_1": engagements
    })
    dd.to_csv("data_dictionary.csv", index=False)

    print("Generated CSVs:")
    for fn in ["students_1.csv","employers_1.csv","jobs_1.csv","events_1.csv","alumni_1.csv","event_attendance_1.csv","engagements_1.csv","data_dictionary.csv"]:
        print(" -", fn)

if __name__ == "__main__":
    main()
