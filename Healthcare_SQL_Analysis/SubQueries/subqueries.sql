/*FROM & JOIN*/
/*Patient born after 2000-01-01
  AND name starts with m
 */

/*Regular Query*/
SELECT *
FROM patients
WHERE date_of_birth>='2000-01-01'
    AND length(master_patient_id::text)>3
    AND  name ilike  'm%'
ORDER BY master_patient_id;

--Subquery
SELECT *
FROM
    (
    SELECT *
    FROM patients
    WHERE date_of_birth>='2000-01-01'
    AND length(master_patient_id::text)>3
    ORDER BY master_patient_id)p
WHERE p.name ilike  'm%';


SELECT se.*
FROM (
    SELECT *
    FROM surgical_encounters
    WHERE surgical_admission_date
        BETWEEN '2016-11-01'
        AND '2016-11-30' ) se
INNER JOIN (
    SELECT master_patient_id
    FROM patients
    WHERE date_of_birth >='1990-01-01'
) p ON se.master_patient_id=p.master_patient_id;


/*IN SELECT*/
SELECT master_patient_id,
       surgery_id,
       (SELECT name
            FROM patients
        WHERE patients.master_patient_id=surgical_encounters.master_patient_id)
       AS patient_name,
       surgical_type,
       total_cost,
       unit_name
FROM surgical_encounters;


/*USING FROM SUBQUERY*/
SELECT *
FROM
(SELECT master_patient_id,
       surgery_id,
       surgical_admission_date,
       surgical_discharge_date,
       (SELECT name
            FROM patients
        WHERE patients.master_patient_id=surgical_encounters.master_patient_id)
       AS patient_name,
       surgical_type,
       total_cost,
       unit_name
FROM surgical_encounters) t
WHERE patient_name = 'Barry Killoran'
ORDER BY surgical_admission_date DESC ;

/*IN WHERE*/
SELECT se.master_patient_id,
       p.name as patient,
       se.surgery_id,
       se.surgical_type,
       se.diagnosis_description,
       se.surgical_admission_date,
       se.surgical_discharge_date,
       se.surgeon_id,
       ph.full_name AS surgeon
FROM surgical_encounters se
INNER JOIN physicians ph
ON se.surgeon_id=ph.id
INNER JOIN patients p
    ON p.master_patient_id=se.master_patient_id
WHERE surgeon_id IN
    (SELECT id from physicians)
AND surgeon_id ='4045'
AND se.master_patient_id='103964'







