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
