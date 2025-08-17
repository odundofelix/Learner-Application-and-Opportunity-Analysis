# 🎓 Learner Application & Opportunity Dashboard  
## 📌 Overview
This project was developed during my internship at **Excelerate**, where we were tasked with creating a unified data model and dashboard to track the complete learner journey — from application to cohort assignment and engagement insights.

Using raw, disconnected datasets, we engineered a clean and relational structure, built a centralized master table, and designed a **Looker Studio dashboard** for real-time insights. The final dashboard supports data-driven decisions to reduce dropout rates and improve learner engagement.

---

## 🎯 Objectives

- ✅ Build a **centralized master table** by integrating five learner-related datasets.
- ✅ Perform **data cleaning, transformation, and relationship mapping** to ensure consistency and usability.
- ✅ Analyze the **marketing dataset separately** due to lack of relational joins.
- ✅ Visualize key metrics in a **Looker Studio dashboard** to uncover trends and gaps.
- ✅ Deliver actionable **recommendations** to stakeholders.

---

## 🧩 Datasets Used

| Dataset Name               | Purpose                                   | Integrated? |
|---------------------------|-------------------------------------------|-------------|
| `cognito_raw2`            | Core learner data (demographics, email)   | ✅ Yes       |
| `learner_raw`             | Education background                      | ✅ Yes       |
| `learner_opportunity_raw` | Links learners to opportunities and cohorts | ✅ Yes    |
| `opportunity_raw`         | Opportunity details (type, category)      | ✅ Yes       |
| `cohort_raw`              | Cohort scheduling                         | ✅ Yes       |
| `marketing_raw`           | Marketing outreach                        | ❌ No        |

---

## 🛠️ Data Processing Workflow

The ETL process was executed using PostgreSQL with the following steps:

1. **Data Extraction**: Loaded all six raw datasets into staging tables.
2. **Data Cleaning**:
   - Null handling and deduplication
   - Standardized date formats (UNIX to ISO)
   - Normalized encoded strings (e.g., `Don%27t want to specify`)
   - City/state formatting and gender/birth date normalization
3. **Relationship Mapping**:
   - Created join keys across datasets (e.g., learner_id, opportunity_id)
   - Built a **master_user_opportunity** table from five datasets
4. **Data Loading**:
   - Populated the final master table for dashboard integration
5. **Dashboard Integration**:
   - Connected the master table to **Looker Studio**

---

## 📊 Dashboard

The interactive dashboard provides insights on:

- **Gender Distribution**
- **Country-wise Application Trends**
- **Education Background Breakdown**
- **Opportunity Category Popularity**
- **Time-based Application Trends**

🔗 [View Dashboard](https://lookerstudio.google.com/s/uBz933SB_uE)

---

## 🔍 Key Insights & Recommendations

| Insight | Recommendation |
|--------|----------------|
| Male learners dominate applications (59.1K vs. 40.4K) | Increase female participation via scholarships and outreach |
| India and Nigeria lead in applications | Focus marketing on underrepresented regions |
| Graduates and undergraduates are the most active | Engage professionals via targeted seminars |
| Internships dominate opportunity types | Justifies current offering given learner background |
| Steady application trend in 2025 | Continue monitoring as more data arrives |

---

## 👥 Team Members

- Niharika Pandey  
- Sarim Kazi  
- Felix Ochieng  
- Parth Mane  
- Himanshu Durgapal  
- Aparna Agarwal  
- Suman Iqbal  
- Niloy Deb Barma  
- Sanchari Karmakar  

---

## 🏁 Final Notes

This project was a successful collaboration focused on transforming fragmented datasets into a reliable, insightful dashboard for stakeholder decision-making.

---
