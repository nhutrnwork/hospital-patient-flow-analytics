--KPI's
--1.Beds occupied total
--2.Total bed turnover
--3.Total patients
--4.Avg treatment duration

--Chart's
--1.Total patients count over time
--2. Total patients over department
--3.Total overstay patients count
--4.AVG treatment duration

--------------------------------------------------------------------------------
--KPI's
--1.Beds occupied total
CREATE VIEW vw_bed_occupancy AS
SELECT 
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.bed_id END) * 1.0 / COUNT(DISTINCT f.bed_id) * 100 AS bed_occupancy_percent
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key AND p.is_current = 1
GROUP BY p.gender;

-- SELECT TOP 10 * FROM dbo.vw_bed_occupancy;

--2.Total bed turnover
CREATE VIEW vw_bed_turnover_rate AS
SELECT 
    p.gender,
    COUNT(DISTINCT f.fact_key) * 1.0 / COUNT(DISTINCT f.bed_id) AS bed_turnover_rate
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key AND p.is_current = 1
GROUP BY p.gender;

-- SELECT TOP 10 * FROM dbo.vw_bed_turnover_rate;

--3.Total patients
CREATE VIEW vw_patient_demographics AS
SELECT 
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.patient_sk END) AS total_patients
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key AND p.is_current = 1
GROUP BY p.gender;

--SELECT TOP 10 * FROM dbo.vw_patient_demographics;

--4.Avg treatment duration
CREATE VIEW vw_avg_treatment_duration AS
SELECT 
    d.department,
    p.gender,
    AVG(f.length_of_stay_hours) AS avg_treatment_duration
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key AND p.is_current = 1
JOIN dbo.dim_department d ON f.department_sk = d.surrogate_key
GROUP BY d.department, p.gender;

-- SELECT TOP 10 * FROM dbo.vw_avg_treatment_duration;

--Chart's
--1.Total patients count over time
CREATE VIEW vw_patient_volume_trend AS
SELECT 
    f.admission_date,
    p.gender,
    COUNT(DISTINCT f.fact_key) AS patient_count
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key AND p.is_current = 1
GROUP BY f.admission_date, p.gender;

-- SELECT TOP 10 * FROM dbo.vw_patient_volume_trend;


--2. Total patients over department
CREATE VIEW vw_department_inflow AS
SELECT 
    d.department,
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.patient_sk END) AS patient_count
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key AND p.is_current = 1
JOIN dbo.dim_department d ON f.department_sk = d.surrogate_key
GROUP BY d.department, p.gender;

-- SELECT TOP 10 * FROM dbo.vw_department_inflow;

--3.Total overstay patients count
CREATE VIEW vw_overstay_patients AS
SELECT 
    d.department,
    p.gender,
    COUNT(DISTINCT f.patient_sk) AS overstay_count
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key AND p.is_current = 1
JOIN dbo.dim_department d ON f.department_sk = d.surrogate_key
WHERE f.length_of_stay_hours > 50
GROUP BY d.department, p.gender;

--SELECT TOP 10 * FROM dbo.vw_overstay_patients;

--4.AVG treatment duration