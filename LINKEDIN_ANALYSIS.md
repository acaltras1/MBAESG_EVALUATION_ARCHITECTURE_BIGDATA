# LinkedIn Jobs Analysis — Snowflake

Ce projet consiste à analyser les offres d'emploi LinkedIn
à partir de fichiers CSV et JSON stockés sur AWS S3.
Les données sont disponibles sur ce bucket : **s3://snowflake-lab-bucket/**

Voici la liste des fichiers à charger :

* job_postings.csv
* benefits.csv
* companies.json
* company_industries.json
* company_specialities.json
* employee_counts.csv
* job_industries.json
* job_skills.csv

Pour pouvoir charger les données dans Snowflake, il faut :
* Créer une base de données **LINKEDIN**
* Créer les schémas **BRONZE**, **SILVER** et **GOLD**
* Créer un stage vers les données sur AWS S3
* Créer un file format pour les données CSV et JSON
* Créer une table pour stocker chaque fichier

---

## 🗄️ Architecture Médaillon

| Couche | Schéma | Rôle |
|--------|--------|------|
| 🟤 Bronze | `LINKEDIN.BRONZE` | Données brutes chargées depuis S3 |
| ⚪ Silver | `LINKEDIN.SILVER` | Données nettoyées et typées |
| 🟡 Gold | `LINKEDIN.GOLD` | Données agrégées prêtes à visualiser |

---

## Étape 1 — Setup

Pour pouvoir charger les données dans Snowflake, il faut créer la base de données, 
les schémas, le stage externe S3 et les formats de fichiers.

```sql
-- Création de la base de données
CREATE DATABASE IF NOT EXISTS LINKEDIN;

-- Création des 3 schémas (Architecture Médaillon)
CREATE SCHEMA IF NOT EXISTS LINKEDIN.BRONZE;
CREATE SCHEMA IF NOT EXISTS LINKEDIN.SILVER;
CREATE SCHEMA IF NOT EXISTS LINKEDIN.GOLD;

-- Création du stage externe pointant vers le bucket S3
CREATE OR REPLACE STAGE LINKEDIN.BRONZE.linkedin_stage
    URL = 's3://snowflake-lab-bucket/';

-- Format pour les fichiers CSV
CREATE OR REPLACE FILE FORMAT LINKEDIN.BRONZE.csv_format
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '');

-- Format pour les fichiers JSON
CREATE OR REPLACE FILE FORMAT LINKEDIN.BRONZE.json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

-- Vérification du contenu du stage
LIST @LINKEDIN.BRONZE.linkedin_stage;
```

> Le `LIST` retourne 8 fichiers confirmant que la connexion S3 est opérationnelle.

## Étape 2 — Création des tables BRONZE

Dans la couche BRONZE, toutes les colonnes sont stockées en `STRING` 
ou `VARIANT` (pour les JSON) afin de conserver les données brutes 
sans aucune transformation.

```sql
USE SCHEMA LINKEDIN.BRONZE;

-- Table job_postings
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_POSTINGS (
    job_id STRING,
    company_name STRING,
    title STRING,
    description STRING,
    max_salary STRING,
    med_salary STRING,
    min_salary STRING,
    pay_period STRING,
    formatted_work_type STRING,
    location STRING,
    applies STRING,
    original_listed_time STRING,
    remote_allowed STRING,
    views STRING,
    job_posting_url STRING,
    application_url STRING,
    application_type STRING,
    expiry STRING,
    closed_time STRING,
    formatted_experience_level STRING,
    skills_desc STRING,
    listed_time STRING,
    posting_domain STRING,
    sponsored STRING,
    work_type STRING,
    currency STRING,
    compensation_type STRING
);

-- Table benefits
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.BENEFITS (
    job_id STRING,
    inferred STRING,
    type STRING
);

-- Table companies (JSON → VARIANT)
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANIES (
    data VARIANT
);

-- Table company_industries (JSON → VARIANT)
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANY_INDUSTRIES (
    data VARIANT
);

-- Table company_specialities (JSON → VARIANT)
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANY_SPECIALITIES (
    data VARIANT
);

-- Table employee_counts
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.EMPLOYEE_COUNTS (
    company_id STRING,
    employee_count STRING,
    follower_count STRING,
    time_recorded STRING
);

-- Table job_industries (JSON → VARIANT)
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_INDUSTRIES (
    data VARIANT
);

-- Table job_skills
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_SKILLS (
    job_id STRING,
    skill_abr STRING
);

-- Vérification des tables créées
SHOW TABLES IN SCHEMA LINKEDIN.BRONZE;
```

> Le `SHOW TABLES` confirme la création des 8 tables dans `LINKEDIN.BRONZE`.

## Étape 3 — Chargement des données BRONZE

Utilisation de `COPY INTO` pour charger chaque fichier depuis le stage S3 
vers les tables BRONZE correspondantes.

```sql
USE SCHEMA LINKEDIN.BRONZE;

-- Chargement de job_postings.csv
COPY INTO LINKEDIN.BRONZE.JOB_POSTINGS
FROM @LINKEDIN.BRONZE.linkedin_stage/job_postings.csv
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.csv_format');

-- Chargement de benefits.csv
COPY INTO LINKEDIN.BRONZE.BENEFITS
FROM @LINKEDIN.BRONZE.linkedin_stage/benefits.csv
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.csv_format');

-- Chargement de employee_counts.csv
COPY INTO LINKEDIN.BRONZE.EMPLOYEE_COUNTS
FROM @LINKEDIN.BRONZE.linkedin_stage/employee_counts.csv
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.csv_format');

-- Chargement de job_skills.csv
COPY INTO LINKEDIN.BRONZE.JOB_SKILLS
FROM @LINKEDIN.BRONZE.linkedin_stage/job_skills.csv
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.csv_format');

-- Chargement de companies.json
COPY INTO LINKEDIN.BRONZE.COMPANIES
FROM @LINKEDIN.BRONZE.linkedin_stage/companies.json
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.json_format');

-- Chargement de company_industries.json
COPY INTO LINKEDIN.BRONZE.COMPANY_INDUSTRIES
FROM @LINKEDIN.BRONZE.linkedin_stage/company_industries.json
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.json_format');

-- Chargement de company_specialities.json
COPY INTO LINKEDIN.BRONZE.COMPANY_SPECIALITIES
FROM @LINKEDIN.BRONZE.linkedin_stage/company_specialities.json
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.json_format');

-- Chargement de job_industries.json
COPY INTO LINKEDIN.BRONZE.JOB_INDUSTRIES
FROM @LINKEDIN.BRONZE.linkedin_stage/job_industries.json
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.json_format');
```

> Résultats du chargement :

| Table | Nombre de lignes |
|-------|-----------------|
| `JOB_POSTINGS` | 15 886 |
| `BENEFITS` | 13 761 |
| `EMPLOYEE_COUNTS` | 15 907 |
| `JOB_SKILLS` | 27 899 |
| `COMPANIES` | 6 063 |
| `COMPANY_INDUSTRIES` | 15 880 |
| `COMPANY_SPECIALITIES` | 128 355 |
| `JOB_INDUSTRIES` | 21 993 |

## Étape 4 — Couche SILVER (Transformation et nettoyage)

Dans la couche SILVER, on nettoie et type correctement chaque colonne.
Les fichiers JSON sont aplatis via la notation `data:colonne::TYPE`.
Les colonnes booléennes contenant `1.0`/`0.0` sont converties via `CASE WHEN`.
Les timestamps Unix sont convertis en dates lisibles via `TO_TIMESTAMP`.

```sql
USE SCHEMA LINKEDIN.SILVER;

-- Table JOB_POSTINGS nettoyée
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_POSTINGS AS
SELECT
    job_id::INT                                    AS job_id,
    company_name::STRING                           AS company_name,
    title::STRING                                  AS title,
    description::STRING                            AS description,
    max_salary::FLOAT                              AS max_salary,
    med_salary::FLOAT                              AS med_salary,
    min_salary::FLOAT                              AS min_salary,
    pay_period::STRING                             AS pay_period,
    formatted_work_type::STRING                    AS work_type,
    location::STRING                               AS location,
    applies::INT                                   AS applies,
    TO_TIMESTAMP(original_listed_time::INT / 1000) AS original_listed_time,
    CASE
        WHEN remote_allowed = '1.0' THEN TRUE
        WHEN remote_allowed = '0.0' THEN FALSE
        ELSE NULL
    END                                            AS remote_allowed,
    views::INT                                     AS views,
    job_posting_url::STRING                        AS job_posting_url,
    application_url::STRING                        AS application_url,
    application_type::STRING                       AS application_type,
    TO_TIMESTAMP(expiry::INT / 1000)               AS expiry,
    TO_TIMESTAMP(closed_time::INT / 1000)          AS closed_time,
    formatted_experience_level::STRING             AS experience_level,
    skills_desc::STRING                            AS skills_desc,
    TO_TIMESTAMP(listed_time::INT / 1000)          AS listed_time,
    posting_domain::STRING                         AS posting_domain,
    CASE
        WHEN sponsored = '1.0' THEN TRUE
        WHEN sponsored = '0.0' THEN FALSE
        ELSE NULL
    END                                            AS sponsored,
    currency::STRING                               AS currency,
    compensation_type::STRING                      AS compensation_type
FROM LINKEDIN.BRONZE.JOB_POSTINGS;

-- Table BENEFITS nettoyée
CREATE OR REPLACE TABLE LINKEDIN.SILVER.BENEFITS AS
SELECT
    job_id::INT AS job_id,
    CASE
        WHEN inferred = '1.0' THEN TRUE
        WHEN inferred = '0.0' THEN FALSE
        ELSE NULL
    END         AS inferred,
    type::STRING AS type
FROM LINKEDIN.BRONZE.BENEFITS;

-- Table COMPANIES extraite du JSON
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANIES AS
SELECT
    data:company_id::INT     AS company_id,
    data:name::STRING        AS name,
    data:description::STRING AS description,
    data:company_size::INT   AS company_size,
    data:state::STRING       AS state,
    data:country::STRING     AS country,
    data:city::STRING        AS city,
    data:zip_code::STRING    AS zip_code,
    data:address::STRING     AS address,
    data:url::STRING         AS url
FROM LINKEDIN.BRONZE.COMPANIES;

-- Table COMPANY_INDUSTRIES extraite du JSON
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANY_INDUSTRIES AS
SELECT
    data:company_id::INT   AS company_id,
    data:industry::STRING  AS industry
FROM LINKEDIN.BRONZE.COMPANY_INDUSTRIES;

-- Table COMPANY_SPECIALITIES extraite du JSON
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANY_SPECIALITIES AS
SELECT
    data:company_id::INT    AS company_id,
    data:speciality::STRING AS speciality
FROM LINKEDIN.BRONZE.COMPANY_SPECIALITIES;

-- Table EMPLOYEE_COUNTS nettoyée
CREATE OR REPLACE TABLE LINKEDIN.SILVER.EMPLOYEE_COUNTS AS
SELECT
    company_id::INT                         AS company_id,
    employee_count::INT                     AS employee_count,
    follower_count::INT                     AS follower_count,
    TO_TIMESTAMP(time_recorded::INT / 1000) AS time_recorded
FROM LINKEDIN.BRONZE.EMPLOYEE_COUNTS;

-- Table JOB_INDUSTRIES extraite du JSON
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_INDUSTRIES AS
SELECT
    data:job_id::INT      AS job_id,
    data:industry_id::INT AS industry_id
FROM LINKEDIN.BRONZE.JOB_INDUSTRIES;

-- Table JOB_SKILLS nettoyée
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_SKILLS AS
SELECT
    job_id::INT       AS job_id,
    skill_abr::STRING AS skill_abr
FROM LINKEDIN.BRONZE.JOB_SKILLS;

-- Vérification globale
SHOW TABLES IN SCHEMA LINKEDIN.SILVER;
```

> **Problème rencontré :** Les colonnes booléennes `remote_allowed`, `sponsored`
> et `inferred` contenaient des valeurs `1.0`/`0.0` non reconnues par Snowflake.
> **Solution :** Utilisation d'un `CASE WHEN` pour convertir manuellement ces valeurs.
