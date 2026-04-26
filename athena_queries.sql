-- Stage 1 Query 1: Extract key facility metrics from nested JSON
SELECT
  facility_id,
  facility_name,
  employee_count,
  cardinality(services)                          AS number_of_offered_services,
  accreditations[1].valid_until                  AS expiry_date_of_first_accreditation,
  location.state                                 AS state
FROM healthcare_db.facilities;

-- Stage 1 Query 2: Count accredited facilities per state
SELECT
  location.state           AS state,
  COUNT(*)                 AS accredited_facility_count
FROM healthcare_db.facilities
WHERE cardinality(accreditations) > 0
GROUP BY location.state
ORDER BY accredited_facility_count DESC;