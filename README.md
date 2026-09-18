#Employee/HR Data Quality Pipeline

This project implements an end-to-end Employee/HR Data Quality and ETL pipeline using PySpark and Delta Lake, following a Bronze → Silver → Gold architecture.

The pipeline processes raw employee data, applies data cleansing and standardization rules, validates critical HR fields, quarantines invalid records with rule-level failure details, and produces a clean, analytics-ready Gold dataset.

Key Objectives
Clean and standardize raw Employee/HR data using PySpark.
Implement field-level data-quality validation rules.
Quarantine invalid records while preserving the original values and failure reasons.
Create validated Silver datasets in Delta format.
Handle duplicate employee records.
Build a business-ready Gold dataset with HR analytics attributes.
Demonstrate an end-to-end data engineering workflow using PySpark, Delta Lake, and data-quality controls.

#Architecture

The pipeline follows a Medallion-style Bronze → Silver → Gold architecture.

                 Raw Employee Data
                        │
                        ▼
              ┌───────────────────┐
              │      BRONZE       │
              │   Raw HR Data     │
              └─────────┬─────────┘
                        │
                        ▼
              ┌───────────────────┐
              │      SILVER       │
              │                   │
              │ • Data Cleaning   │
              │ • Standardization │
              │ • Validation      │
              │ • DQ Rules        │
              └───────┬───┬───────┘
                      │   │
             Valid    │   │    Invalid
                      │   │
                      ▼   ▼
              ┌─────────┐ ┌──────────────┐
              │  Final  │ │ Quarantine   │
              │ Silver  │ │    Silver    │
              └────┬────┘ └──────────────┘
                   │
                   ▼
            ┌───────────────────┐
            │       GOLD        │
            │                   │
            │ • HR-ready data   │
            │ • Employee Tenure │
            │ • Salary Band     │
            │ • Experience Cat. │
            └───────────────────┘
Layer Responsibilities

Bronze

Stores the raw Employee/HR source data.
Preserves the original source values for downstream processing and traceability.

Silver

Cleans and standardizes employee attributes.
Applies field-level data-quality rules.
Generates validation flags.
Separates valid and invalid records.
Stores invalid records in a dedicated quarantine dataset with rule-level failure information.

Gold

Uses the validated employee records from Silver.
Removes technical DQ columns and raw fields.
Adds business-oriented HR attributes.
Produces a clean dataset suitable for analytics and BI reporting.
Silver Outputs

The Silver layer maintains three Delta outputs:

silver/
├── cleaned/
├── final/
└── quarantined/
Gold Output

The Gold layer contains the final analytics-ready Employee dataset in Delta format.

#Dataset

The pipeline processes an Employee/HR dataset containing employee identity, contact, organizational, compensation, employment, and reporting information.

Employee Fields
Employee_ID
Employee_Name
Email
Phone
Department
Job_Title
Salary
Joining_Date
Employee_Status
Manager_ID
Data Quality Rules

The Silver layer applies 10 field-level data-quality rules. Each validation generates a corresponding Valid_<Column> flag, and records failing a rule are captured in the quarantine dataset.

Rule ID	Field	Validation
DQ001	Employee_ID	Validates the Employee ID format and ensures the identifier conforms to the defined pattern.
DQ002	Employee_Name	Validates employee name format after trimming and standardization.
DQ003	Email	Validates the standardized email format.
DQ004	Phone	Validates the cleaned phone number format.
DQ005	Department	Validates the standardized department value.
DQ006	Job_Title	Validates the standardized job-title value.
DQ007	Salary	Validates and standardizes salary values into a numeric representation.
DQ008	Joining_Date	Validates and converts joining-date values into a date representation.
DQ009	Employee_Status	Validates the employee employment-status value.
DQ010	Manager_ID	Validates the Manager ID format.
Validation Handling

A record is considered valid for the final Silver dataset when it passes all applicable data-quality validations.

Records failing validation are retained separately in the quarantine dataset with:

Employee_ID
Column_Name
Invalid_Value
Rule_ID
Failure_Reason

This approach preserves invalid source values and provides traceability for each data-quality failure.

#Gold Layer

The Gold layer is built from the validated Silver employee dataset and contains clean, business-ready employee information for analytics and reporting.

Gold Layer Objectives
Remove technical data-quality columns and raw source fields.
Retain only validated employee records.
Create business-oriented HR attributes.
Produce a clean dataset suitable for Power BI, reporting, and analytics.
Gold Transformations

The following business transformations are applied:

1. Employee Tenure

Calculates the employee's approximate tenure in years based on the Joining_Date.

Column: Employee_Tenure

2. Salary Band

Employees are categorized into salary bands based on annual salary:

Salary Range	Salary Band
< ₹3,00,000	Low
₹3,00,000 – ₹5,99,999	Lower-Middle
₹6,00,000 – ₹9,99,999	Middle
₹10,00,000 – ₹14,99,999	Upper-Middle
>= ₹15,00,000	High

Column: Salary_Band

3. Experience Category

Employees are grouped according to their calculated tenure:

Tenure	Experience Category
< 2 years	Fresher
2 – < 5 years	Early Career
5 – < 10 years	Mid Career
>= 10 years	Experienced

Column: Experience_Category

Final Gold Schema

The Gold dataset contains:

Employee identity and contact information
Organizational information
Salary and employment information
Reporting information
Employee tenure
Salary band
Experience category

The final Gold dataset contains 22 validated employee records with no duplicate Employee_ID values.

Gold Output

The Gold dataset is persisted in Delta format and serves as the final analytics-ready output of the pipeline.

#Duplicate Handling and Validation
Duplicate Handling

The pipeline checks for duplicate employee records before producing the final Gold dataset.

An exact duplicate Employee record was identified and removed during the data-quality processing.

The final Gold dataset was then validated to ensure that:

Employee_ID values are unique.
No unintended duplicate employee records remain.
Validated Silver records are correctly propagated to Gold.
Final Gold Validation

The Gold dataset was validated using the following checks:

Validation Check	Result
Total Employee Records	22
Duplicate Employee_ID	0
Non-null Employee_ID	22
Non-null Employee_Name	22
Non-null Email	22
Non-null Phone	22
Non-null Department	22
Non-null Job_Title	22
Non-null Salary	22
Non-null Joining_Date	22
Non-null Employee_Status	22
Non-null Manager_ID	22

These checks confirm that the final Gold dataset contains 22 clean, validated, and unique employee records ready for downstream analytics.

#Technologies
Python — Data engineering and transformation logic
PySpark — Data cleaning, transformation, validation, and analysis
Apache Spark — Distributed data processing
Delta Lake — Reliable storage for Silver and Gold datasets
Databricks — Data engineering and ETL development environment
SQL / Spark SQL — Data querying and validation
GitHub — Project version control and portfolio documentation
Key Outcomes

This project demonstrates the ability to build an end-to-end Employee/HR data pipeline with a strong focus on data quality and analytics readiness.

Key capabilities demonstrated:

Designed a Bronze → Silver → Gold data pipeline.
Cleaned and standardized messy HR source data.
Implemented 10 field-level data-quality rules.
Created validation flags for individual data fields.
Implemented a structured quarantine mechanism for invalid records.
Preserved original values for data-quality investigation and traceability.
Used Delta Lake for persistent Silver and Gold datasets.
Handled duplicate employee records.
Created business-oriented HR attributes such as tenure, salary band, and experience category.
Performed final data-quality and uniqueness validation.
Produced an analytics-ready Gold dataset containing 22 validated employee records.

#Project Structure
Employee-HR-Data-Quality-Pipeline/
│
├── Employee_HR_Data_Quality_Pipeline.ipynb
├── README.md
├── requirements.txt
│
├── data/
│   └── employee_hr_raw.csv
│
└── delta/
    ├── bronze/
    ├── silver/
    │   ├── cleaned/
    │   ├── final/
    │   └── quarantined/
    │
    └── gold/

The exact local folder structure may vary depending on the execution environment. The structure above represents the logical organization of the project.

How to Run
1. Clone the Repository

Clone this repository to your local machine or open the notebook directly in a compatible Databricks/PySpark environment.

2. Install Dependencies

Install the packages listed in requirements.txt.

3. Open the Notebook

Open:

Employee_HR_Data_Quality_Pipeline.ipynb

4. Run the Pipeline

Execute the notebook sequentially from top to bottom.

The notebook performs the following workflow:

Raw Employee Data → Bronze → Silver Cleaning → Data Quality Validation → Quarantine → Silver Final → Gold Transformation → Delta Output

5. Review the Outputs

After successful execution, review:

Cleaned Silver dataset
Final validated Silver dataset
Quarantined records
Gold analytics-ready dataset
Expected Result

The completed pipeline produces a validated Gold dataset containing 22 unique employee records, along with a separate quarantine dataset containing records that failed the defined data-quality rules.
