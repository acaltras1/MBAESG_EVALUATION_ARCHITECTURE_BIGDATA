# MBAESG_EVALUATION_ARCHITECTURE_BIGDATA
Analyse des offres d'emploi LinkedIn avec Snowflake

# 🧊 Analyse des Offres d'Emploi LinkedIn avec Snowflake

Ce projet s'inscrit dans le cadre d'une analyse du marché de l'emploi à partir de données réelles extraites de LinkedIn. L'objectif est de construire un pipeline de données complet, de l'ingestion des données brutes jusqu'à la visualisation des résultats.

---

## 📌 Table des matières

1. [Présentation du projet](#présentation-du-projet)
2. [Architecture des données](#architecture-des-données)
3. [Jeu de données](#jeu-de-données)
4. [Modèle relationnel](#modèle-relationnel)
5. [Stack technique](#stack-technique)
6. [Structure du projet](#structure-du-projet)
7. [Pipeline de données](#pipeline-de-données)
8. [Analyses et visualisations](#analyses-et-visualisations)
9. [Résultats obtenus](#résultats-obtenus)
10. [Auteurs](#auteurs)

---

## Présentation du projet

Ce projet consiste à construire un pipeline de données complet à partir d'un jeu de données LinkedIn. Les données brutes au format CSV et JSON sont stockées dans un bucket AWS S3 public et chargées dans Snowflake via des scripts SQL exclusivement. Des analyses sont ensuite réalisées et visualisées avec Streamlit.

Le projet se déroule en 4 grandes phases :

### 1️⃣ Ingestion des données
Les données sources sont disponibles sous forme de fichiers CSV et JSON dans un bucket AWS S3 public : `s3://snowflake-lab-bucket/`. Elles couvrent plusieurs milliers d'offres d'emploi et incluent des informations sur les postes, les entreprises, les compétences, les salaires et les avantages sociaux.

### 2️⃣ Chargement dans Snowflake
Les fichiers sont chargés dans un entrepôt de données Snowflake via des scripts SQL exclusivement. Cette phase comprend la création de la base de données, la configuration d'un stage externe pointant vers S3, la définition des formats de fichiers et le chargement des données dans des tables structurées.

### 3️⃣ Transformation et nettoyage
Une fois les données chargées, des transformations sont appliquées pour garantir la cohérence et l'intégrité des données : conversion des timestamps Unix, gestion des valeurs nulles, typage des colonnes et mise en relation des différentes tables.

### 4️⃣ Analyse et visualisation
Cinq analyses sont réalisées sur les données pour explorer le marché de l'emploi, chacune accompagnée d'une visualisation interactive créée avec Streamlit directement dans Snowflake.

---

## Architecture des données

Ce projet suit l'**Architecture Médaillon** en 3 couches :

```
┌─────────────────────────────────────────────────────────┐
│                      AWS S3 Bucket                       │
│              s3://snowflake-lab-bucket/                  │
│   CSV : job_postings, benefits, employee_counts,         │
│         job_skills                                       │
│   JSON : companies, company_industries,                  │
│          company_specialities, job_industries            │
└───────────────────────┬─────────────────────────────────┘
                        │  COPY INTO
                        ▼
┌─────────────────────────────────────────────────────────┐
│                  🟤 COUCHE BRONZE                        │
│                  LINKEDIN.BRONZE                         │
│   Données brutes — tout stocké en STRING ou VARIANT      │
│   Aucune transformation, fidèle à la source              │
└───────────────────────┬─────────────────────────────────┘
                        │  CREATE TABLE AS SELECT
                        ▼
┌─────────────────────────────────────────────────────────┐
│                  ⚪ COUCHE SILVER                        │
│                  LINKEDIN.SILVER                         │
│   Données nettoyées — typage, renommage, gestion nulls   │
│   Timestamps convertis, colonnes correctement typées     │
└───────────────────────┬─────────────────────────────────┘
                        │  CREATE VIEW / CREATE TABLE AS
                        ▼
┌─────────────────────────────────────────────────────────┐
│                  🟡 COUCHE GOLD                          │
│                  LINKEDIN.GOLD                           │
│   Données agrégées — jointures, analyses, KPIs           │
│   Prêtes pour les visualisations Streamlit               │
└─────────────────────────────────────────────────────────┘
```

| Couche | Schéma | Rôle |
|--------|--------|------|
| 🟤 **Bronze** | `LINKEDIN.BRONZE` | Données brutes chargées depuis S3 |
| ⚪ **Silver** | `LINKEDIN.SILVER` | Données nettoyées et typées |
| 🟡 **Gold** | `LINKEDIN.GOLD` | Données agrégées prêtes à visualiser |

---

## Jeu de données

Les fichiers sont hébergés dans le bucket S3 public : `s3://snowflake-lab-bucket/`

| Fichier | Format | Description |
|---------|--------|-------------|
| `job_postings.csv` | CSV | Fichier central — toutes les offres d'emploi |
| `benefits.csv` | CSV | Avantages associés à chaque offre |
| `job_skills.csv` | CSV | Compétences requises par offre |
| `employee_counts.csv` | CSV | Nombre d'employés et followers par entreprise |
| `companies.json` | JSON | Informations détaillées sur les entreprises |
| `company_industries.json` | JSON | Secteurs d'activité des entreprises |
| `company_specialities.json` | JSON | Spécialités des entreprises |
| `job_industries.json` | JSON | Secteurs d'activité des offres |

---

## Modèle relationnel

```
                    ┌─────────────────┐
                    │   JOB_POSTINGS  │
                    │   (table centrale)│
                    └────────┬────────┘
                             │ job_id
          ┌──────────────────┼──────────────────┐
          │                  │                  │
          ▼                  ▼                  ▼
   ┌─────────────┐   ┌──────────────┐   ┌─────────────────┐
   │  BENEFITS   │   │  JOB_SKILLS  │   │ JOB_INDUSTRIES  │
   └─────────────┘   └──────────────┘   └─────────────────┘

                    ┌─────────────────┐
                    │    COMPANIES    │
                    └────────┬────────┘
                             │ company_id
          ┌──────────────────┼──────────────────┐
          │                  │                  │
          ▼                  ▼                  ▼
 ┌────────────────┐  ┌───────────────────┐  ┌──────────────────┐
 │EMPLOYEE_COUNTS │  │COMPANY_INDUSTRIES │  │COMPANY_SPECIALITIES│
 └────────────────┘  └───────────────────┘  └──────────────────┘
```

---

## Stack technique

| Outil | Version | Rôle |
|-------|---------|------|
| **Snowflake** | Enterprise | Entrepôt de données, exécution SQL |
| **AWS S3** | — | Stockage des fichiers sources |
| **Streamlit** | Intégré Snowflake | Visualisation interactive |
| **SQL** | Snowflake SQL | Manipulation et transformation des données |
| **Python** | 3.x | Code Streamlit |
| **GitHub** | — | Versioning et livrable final |

---

## Structure du projet

```
MBAESG_EVALUATION_ARCHITECTURE_BIGDATA/
│
├── README.md                    ← Présentation visuelle du projet (ce fichier)
│
└── LINKEDIN_ANALYSIS.md         ← Code complet SQL + Python + explications
```

---

## Pipeline de données

```
S3 Bucket
    │
    ├── job_postings.csv  ──►  BRONZE.JOB_POSTINGS  ──►  SILVER.JOB_POSTINGS  ──►  GOLD.TOP_TITLES
    ├── benefits.csv      ──►  BRONZE.BENEFITS       ──►  SILVER.BENEFITS
    ├── job_skills.csv    ──►  BRONZE.JOB_SKILLS     ──►  SILVER.JOB_SKILLS
    ├── employee_counts   ──►  BRONZE.EMPLOYEE_COUNTS──►  SILVER.EMPLOYEE_COUNTS
    ├── companies.json    ──►  BRONZE.COMPANIES      ──►  SILVER.COMPANIES     ──►  GOLD.TOP_SALARIES
    ├── company_ind.json  ──►  BRONZE.COMPANY_IND    ──►  SILVER.COMPANY_IND
    ├── company_spe.json  ──►  BRONZE.COMPANY_SPE    ──►  SILVER.COMPANY_SPE
    └── job_ind.json      ──►  BRONZE.JOB_IND        ──►  SILVER.JOB_IND       ──►  GOLD.BY_SECTOR
```

---

## Analyses et visualisations

| # | Analyse | Type de graphique |
|---|---------|------------------|
| 1 | Top 10 des titres de postes les plus publiés par industrie | Graphique en barres |
| 2 | Top 10 des postes les mieux rémunérés par industrie | Graphique en barres horizontales |
| 3 | Répartition des offres par taille d'entreprise | Graphique en camembert |
| 4 | Répartition des offres par secteur d'activité | Graphique en barres |
| 5 | Répartition des offres par type d'emploi | Graphique en camembert |

---

## Résultats obtenus

> 📸 Les captures d'écran des visualisations Streamlit seront ajoutées ici à la fin du projet.

---

## Auteurs

> Projet réalisé dans le cadre du cours **Architecture Big Data** — MBA ESG
> Binôme : **Franck Jordan WAFO** & **Ibrahim-Khalil Meleissy CISSE**
