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

## Étape 5 — Couche GOLD (Analyses)

Les 10 analyses sont créées sous forme de vues dans la couche GOLD,
prêtes à être consommées par Streamlit.

### Description des vues GOLD

| Vue | Description |
|-----|-------------|
| `TOP_TITLES_BY_INDUSTRY` | Top 10 des titres de postes les plus publiés par industrie |
| `TOP_SALARIES_BY_INDUSTRY` | Top 10 des postes les mieux rémunérés par industrie |
| `POSTINGS_BY_COMPANY_SIZE` | Répartition des offres par taille d'entreprise |
| `POSTINGS_BY_INDUSTRY` | Répartition des offres par secteur d'activité |
| `POSTINGS_BY_WORK_TYPE` | Répartition des offres par type d'emploi |
| `TOP_RECRUITING_COMPANIES` | Top 10 des entreprises qui recrutent le plus |
| `POSTINGS_BY_EXPERIENCE` | Répartition des offres par niveau d'expérience |
| `POSTINGS_BY_REMOTE` | Répartition Remote vs Présentiel |
| `TOP_LOCATIONS` | Top 10 des localisations avec le plus d'offres |
| `AVG_SALARY_BY_WORK_TYPE` | Salaire moyen par type de contrat |

---

### Analyse 1 — Top 10 des titres de postes les plus publiés par industrie
Jointure entre `JOB_POSTINGS`, `COMPANIES` et `COMPANY_INDUSTRIES`.
Utilisation de `ROW_NUMBER()` pour classer les titres par industrie.

```sql
CREATE OR REPLACE VIEW LINKEDIN.GOLD.TOP_TITLES_BY_INDUSTRY AS
SELECT
    ci.industry                     AS industry,
    jp.title                        AS job_title,
    COUNT(*)                        AS nb_postings,
    ROW_NUMBER() OVER (
        PARTITION BY ci.industry
        ORDER BY COUNT(*) DESC
    )                               AS rank
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
JOIN LINKEDIN.SILVER.COMPANIES c
    ON jp.company_name::FLOAT::INT = c.company_id
JOIN LINKEDIN.SILVER.COMPANY_INDUSTRIES ci
    ON c.company_id = ci.company_id
GROUP BY ci.industry, jp.title
QUALIFY rank <= 10;
```

---

### Analyse 2 — Top 10 des postes les mieux rémunérés par industrie
Le salaire médian étant absent des données sources, il est calculé
comme la moyenne entre le salaire minimum et maximum.

```sql
CREATE OR REPLACE VIEW LINKEDIN.GOLD.TOP_SALARIES_BY_INDUSTRY AS
SELECT
    ci.industry                                         AS industry,
    jp.title                                            AS job_title,
    ROUND(AVG(jp.max_salary), 2)                        AS avg_max_salary,
    ROUND(AVG((jp.max_salary + jp.min_salary) / 2), 2)  AS avg_med_salary,
    ROUND(AVG(jp.min_salary), 2)                        AS avg_min_salary,
    COUNT(*)                                            AS nb_postings,
    ROW_NUMBER() OVER (
        PARTITION BY ci.industry
        ORDER BY AVG(jp.max_salary) DESC
    )                                                   AS rank
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
JOIN LINKEDIN.SILVER.COMPANIES c
    ON jp.company_name::FLOAT::INT = c.company_id
JOIN LINKEDIN.SILVER.COMPANY_INDUSTRIES ci
    ON c.company_id = ci.company_id
WHERE jp.max_salary IS NOT NULL
AND jp.min_salary IS NOT NULL
GROUP BY ci.industry, jp.title
QUALIFY rank <= 10;
```

---

### Analyse 3 — Répartition des offres par taille d'entreprise
La colonne `company_size` contient des valeurs de 0 à 7.
Un `CASE WHEN` permet de les convertir en labels lisibles.

```sql
CREATE OR REPLACE VIEW LINKEDIN.GOLD.POSTINGS_BY_COMPANY_SIZE AS
SELECT
    CASE c.company_size
        WHEN 0 THEN '0 - Très petite (1-10)'
        WHEN 1 THEN '1 - Petite (11-50)'
        WHEN 2 THEN '2 - Moyenne (51-200)'
        WHEN 3 THEN '3 - Intermédiaire (201-500)'
        WHEN 4 THEN '4 - Grande (501-1000)'
        WHEN 5 THEN '5 - Très grande (1001-5000)'
        WHEN 6 THEN '6 - Très grande (5001-10000)'
        WHEN 7 THEN '7 - Entreprise (10000+)'
        ELSE 'Non renseigné'
    END                             AS company_size_label,
    COUNT(*)                        AS nb_postings
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
JOIN LINKEDIN.SILVER.COMPANIES c
    ON jp.company_name::FLOAT::INT = c.company_id
GROUP BY c.company_size
ORDER BY c.company_size;
```

---

### Analyse 4 — Répartition des offres par secteur d'activité
Jointure entre `JOB_POSTINGS` et `JOB_INDUSTRIES`.
Limite aux 20 secteurs les plus représentés.

```sql
CREATE OR REPLACE VIEW LINKEDIN.GOLD.POSTINGS_BY_INDUSTRY AS
SELECT
    ji.industry_id::STRING          AS industry,
    COUNT(*)                        AS nb_postings
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
JOIN LINKEDIN.SILVER.JOB_INDUSTRIES ji
    ON jp.job_id = ji.job_id
GROUP BY ji.industry_id
ORDER BY nb_postings DESC
LIMIT 20;
```

---

### Analyse 5 — Répartition des offres par type d'emploi
Utilisation de la fonction fenêtre `SUM() OVER()` pour calculer
les pourcentages par rapport au total des offres.

```sql
CREATE OR REPLACE VIEW LINKEDIN.GOLD.POSTINGS_BY_WORK_TYPE AS
SELECT
    work_type                       AS work_type,
    COUNT(*)                        AS nb_postings,
    ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (), 2)   AS percentage
FROM LINKEDIN.SILVER.JOB_POSTINGS
WHERE work_type IS NOT NULL
GROUP BY work_type
ORDER BY nb_postings DESC;
```

---

### Analyse 6 — Top 10 des entreprises qui recrutent le plus

```sql
CREATE OR REPLACE VIEW LINKEDIN.GOLD.TOP_RECRUITING_COMPANIES AS
SELECT
    c.name                          AS company_name,
    COUNT(*)                        AS nb_postings,
    CASE c.company_size
        WHEN 0 THEN 'Très petite (1-10)'
        WHEN 1 THEN 'Petite (11-50)'
        WHEN 2 THEN 'Moyenne (51-200)'
        WHEN 3 THEN 'Intermédiaire (201-500)'
        WHEN 4 THEN 'Grande (501-1000)'
        WHEN 5 THEN 'Très grande (1001-5000)'
        WHEN 6 THEN 'Très grande (5001-10000)'
        WHEN 7 THEN 'Entreprise (10000+)'
        ELSE 'Non renseigné'
    END                             AS company_size_label
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
JOIN LINKEDIN.SILVER.COMPANIES c
    ON jp.company_name::FLOAT::INT = c.company_id
GROUP BY c.name, c.company_size
ORDER BY nb_postings DESC
LIMIT 10;
```

---

### Analyse 7 — Répartition des offres par niveau d'expérience

```sql
CREATE OR REPLACE VIEW LINKEDIN.GOLD.POSTINGS_BY_EXPERIENCE AS
SELECT
    experience_level                AS experience_level,
    COUNT(*)                        AS nb_postings,
    ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (), 2)   AS percentage
FROM LINKEDIN.SILVER.JOB_POSTINGS
WHERE experience_level IS NOT NULL
GROUP BY experience_level
ORDER BY nb_postings DESC;
```

---

### Analyse 8 — Répartition Remote vs Présentiel

```sql
CREATE OR REPLACE VIEW LINKEDIN.GOLD.POSTINGS_BY_REMOTE AS
SELECT
    CASE
        WHEN remote_allowed = TRUE THEN 'Remote'
        ELSE 'Présentiel'
    END                             AS remote_label,
    COUNT(*)                        AS nb_postings,
    ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (), 2)   AS percentage
FROM LINKEDIN.SILVER.JOB_POSTINGS
WHERE remote_allowed IS NOT NULL
GROUP BY remote_allowed
ORDER BY nb_postings DESC;
```

---

### Analyse 9 — Top 10 des localisations avec le plus d'offres

```sql
CREATE OR REPLACE VIEW LINKEDIN.GOLD.TOP_LOCATIONS AS
SELECT
    location                        AS location,
    COUNT(*)                        AS nb_postings,
    ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (), 2)   AS percentage
FROM LINKEDIN.SILVER.JOB_POSTINGS
WHERE location IS NOT NULL
GROUP BY location
ORDER BY nb_postings DESC
LIMIT 10;
```

---

### Analyse 10 — Salaire moyen par type de contrat
Le salaire médian est calculé comme la moyenne entre min et max
car le champ `med_salary` est absent des données sources.

```sql
CREATE OR REPLACE VIEW LINKEDIN.GOLD.AVG_SALARY_BY_WORK_TYPE AS
SELECT
    work_type                                       AS work_type,
    ROUND(AVG(max_salary), 2)                       AS avg_max_salary,
    ROUND(AVG((max_salary + min_salary) / 2), 2)    AS avg_med_salary,
    ROUND(AVG(min_salary), 2)                       AS avg_min_salary,
    COUNT(*)                                        AS nb_postings
FROM LINKEDIN.SILVER.JOB_POSTINGS
WHERE max_salary IS NOT NULL
AND min_salary IS NOT NULL
AND work_type IS NOT NULL
GROUP BY work_type
ORDER BY avg_max_salary DESC;
```
> Résultats de l'analyse 5 — Répartition par type d'emploi :

| Type d'emploi | Nombre d'offres | Pourcentage |
|---------------|----------------|-------------|
| Full-time | 12 844 | 80.85% |
| Contract | 1 739 | 10.95% |
| Part-time | 1 010 | 6.36% |
| Temporary | 121 | 0.70% |
| Internship | 111 | 0.70% |
| Other | 53 | 0.33% |
| Volunteer | 8 | 0.05% |

## Étape 6 — Visualisations Streamlit

Création d'un dashboard interactif avec 5 visualisations
hébergé directement dans Snowflake via Streamlit.

### Problème rencontré
La colonne `company_name` dans `JOB_POSTINGS` contenait en réalité
des IDs numériques au format float (`77766802.0`) et non des noms
d'entreprises. La jointure avec `COMPANIES` nécessitait une conversion
`::FLOAT::INT` pour correspondre au `company_id`.

```python
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("🧊 Analyse des Offres d'Emploi LinkedIn")
st.write("Dashboard interactif basé sur les données LinkedIn — MBA ESG Architecture Big Data")

# Analyse 1 : Top 10 des titres de postes par industrie
st.header("📊 Analyse 1 — Top 10 des titres de postes par industrie")

industries = session.sql("""
    SELECT DISTINCT industry 
    FROM LINKEDIN.GOLD.TOP_TITLES_BY_INDUSTRY
    ORDER BY industry
""").to_pandas()

industry_list = industries["INDUSTRY"].tolist()

selected_industry_1 = st.selectbox(
    "Sélectionne une industrie :",
    industry_list,
    key="industry_1"
)

data_1 = session.sql(f"""
    SELECT job_title, nb_postings
    FROM LINKEDIN.GOLD.TOP_TITLES_BY_INDUSTRY
    WHERE industry = '{selected_industry_1}'
    ORDER BY rank
""").to_pandas()

data_1.columns = [c.lower() for c in data_1.columns]
st.bar_chart(data=data_1, x="job_title", y="nb_postings")
st.subheader("Données brutes")
st.dataframe(data_1, use_container_width=True)

st.divider()

# Analyse 2 : Top 10 des postes les mieux rémunérés
st.header("💰 Analyse 2 — Top 10 des postes les mieux rémunérés par industrie")

selected_industry_2 = st.selectbox(
    "Sélectionne une industrie :",
    industry_list,
    key="industry_2"
)

data_2 = session.sql(f"""
    SELECT job_title, avg_max_salary, avg_med_salary, avg_min_salary
    FROM LINKEDIN.GOLD.TOP_SALARIES_BY_INDUSTRY
    WHERE industry = '{selected_industry_2}'
    ORDER BY rank
""").to_pandas()

data_2.columns = [c.lower() for c in data_2.columns]
st.bar_chart(data=data_2, x="job_title", y="avg_max_salary")
st.subheader("Données brutes")
st.dataframe(data_2, use_container_width=True)

st.divider()

# Analyse 3 : Répartition par taille d'entreprise
st.header("🏢 Analyse 3 — Répartition des offres par taille d'entreprise")

data_3 = session.sql("""
    SELECT company_size_label, nb_postings
    FROM LINKEDIN.GOLD.POSTINGS_BY_COMPANY_SIZE
    ORDER BY company_size_label
""").to_pandas()

data_3.columns = [c.lower() for c in data_3.columns]
st.bar_chart(data=data_3, x="company_size_label", y="nb_postings")
st.subheader("Données brutes")
st.dataframe(data_3, use_container_width=True)

st.divider()

# Analyse 4 : Répartition par secteur d'activité
st.header("🏭 Analyse 4 — Top 20 des secteurs d'activité")

data_4 = session.sql("""
    SELECT industry, nb_postings
    FROM LINKEDIN.GOLD.POSTINGS_BY_INDUSTRY
    ORDER BY nb_postings DESC
""").to_pandas()

data_4.columns = [c.lower() for c in data_4.columns]
st.bar_chart(data=data_4, x="industry", y="nb_postings")
st.subheader("Données brutes")
st.dataframe(data_4, use_container_width=True)

st.divider()

# Analyse 5 : Répartition par type d'emploi
st.header("⏱️ Analyse 5 — Répartition des offres par type d'emploi")

data_5 = session.sql("""
    SELECT work_type, nb_postings, percentage
    FROM LINKEDIN.GOLD.POSTINGS_BY_WORK_TYPE
""").to_pandas()

data_5.columns = [c.lower() for c in data_5.columns]
st.bar_chart(data=data_5, x="work_type", y="nb_postings")
st.subheader("Données brutes")
st.dataframe(data_5, use_container_width=True)
```

> **Résultats :** Les 5 analyses sont fonctionnelles et interactives.
> Les captures d'écran sont disponibles ci-dessous.

![Analyse 1](images/analyse1.png)
![Analyse 2](images/analyse2.png)
![Analyse 3](images/analyse3.png)
![Analyse 4](images/analyse4.png)
![Analyse 5](images/analyse5.png)

## ⚠️ Problèmes rencontrés et solutions apportées

### Problème 1 — Conversion des colonnes booléennes
**Étape concernée :** Couche SILVER — Table `JOB_POSTINGS`

**Erreur obtenue :**
```
DML operation failed on column REMOTE_ALLOWED with error:
Boolean value '1.0' is not recognized
```

**Cause :** Les colonnes `remote_allowed`, `sponsored` et `inferred`
contenaient des valeurs `1.0` et `0.0` au lieu de `TRUE`/`FALSE`.
Snowflake ne reconnaît pas ce format pour un cast direct en BOOLEAN.

**Solution :** Remplacement du cast `::BOOLEAN` par un `CASE WHEN` :
```sql
CASE
    WHEN remote_allowed = '1.0' THEN TRUE
    WHEN remote_allowed = '0.0' THEN FALSE
    ELSE NULL
END AS remote_allowed
```

---

### Problème 2 — Colonnes retournées en majuscules par Snowflake
**Étape concernée :** Visualisations Streamlit

**Erreur obtenue :**
```
StreamlitColumnNotFoundError: Data does not have a column named "JOB_TITLE".
Available columns are ``
```

**Cause :** Snowflake retourne tous les noms de colonnes en majuscules
(`JOB_TITLE`, `NB_POSTINGS`...) alors que Streamlit les cherche
en minuscules dans `st.bar_chart()`.

**Solution :** Conversion des données en DataFrame pandas avec
mise en minuscules des colonnes :
```python
data = session.sql("SELECT ...").to_pandas()
data.columns = [c.lower() for c in data.columns]
```

---

### Problème 3 — Jointure incorrecte entre JOB_POSTINGS et COMPANIES
**Étape concernée :** Couche GOLD — Analyses 1, 2 et 3

**Symptôme :** Les graphiques des analyses 1, 2 et 3 étaient vides,
aucune donnée n'était retournée.

**Cause :** La colonne `company_name` dans `JOB_POSTINGS` ne contient
pas des noms d'entreprises mais des **IDs numériques stockés en float**
(ex: `77766802.0`). La jointure avec `c.name` retournait donc 0 lignes.

**Diagnostic :**
```sql
-- company_name contient des IDs en float, pas des noms
SELECT DISTINCT company_name
FROM LINKEDIN.SILVER.JOB_POSTINGS
LIMIT 5;
-- Résultat : 3895037.0, 4422.0, 3738912.0...

-- La jointure directe retourne 0 lignes
SELECT COUNT(*)
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
JOIN LINKEDIN.SILVER.COMPANIES c
    ON jp.company_name = c.name;
-- Résultat : 0
```

**Solution :** Conversion de `company_name` en INT pour le faire
correspondre au `company_id` de la table `COMPANIES` :
```sql
JOIN LINKEDIN.SILVER.COMPANIES c
    ON jp.company_name::FLOAT::INT = c.company_id
```

---

### Récapitulatif des problèmes

| # | Problème | Étape | Solution |
|---|----------|-------|----------|
| 1 | Booléens `1.0`/`0.0` non reconnus | SILVER | `CASE WHEN` |
| 2 | Colonnes en majuscules dans Streamlit | Streamlit | `.to_pandas()` + `.lower()` |
| 3 | `company_name` contient des IDs float | GOLD | `::FLOAT::INT` |
