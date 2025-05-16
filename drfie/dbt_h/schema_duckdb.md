# Documentation du Schéma DuckDB

**Base de données:** G:/temp/com_drfie_dbt_h.duckdb
**Date de génération:** 16/05/2025 06:22:04

## Résumé des Tables

| Table | Colonnes | Lignes |
|-------|----------|-------|
| FAITS_ADDgpn | 10 | 453445 |
| FAITS_MASSALgpn | 7 | 210811 |
| LABOs_enrichi_bassins_de_vie | 248 | 4671 |
| REFERENTIEL_SITE_ERP | 15 | 1304 |
| csvreport_sfa_all | 4 | Erreur |
| duplicates_SILs_idCorrespondants | 3 | 2 |
| id_pharmacie_mapping | 2 | 11 |
| labos | 3 | 4671 |
| mapping_SILs_idCorrespondants_cps | 2 | 2430 |
| mapping_SILs_idPersonnels_rhs | 2 | 37224 |
| mapping_idsite_labos | 13 | 1919 |
| mapping_pcod_rhs | 2 | 37801 |
| mapping_xplans_codes_rhs | 2 | 14000 |
| metricflow_time_spine | 1 | 4017 |
| sildemanderesultat | 1 | 191308463 |
| silpatient | 7 | 5228887 |
| stg_sfa_monthly | 57 | 273111274 |
| tRHsSIL | 7 | 128083 |

## Détails des Tables

### Table: FAITS_ADDgpn

**Nombre d'enregistrements:** 453445

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| %Salarie | VARCHAR | YES | NULL |
| %CleLBM | VARCHAR | YES | NULL |
| MATRI | INTEGER | YES | NULL |
| xx | VARCHAR | YES | NULL |
| Date | DATE | YES | NULL |
| ANMOIS | VARCHAR | YES | NULL |
| nomKSL | VARCHAR | YES | NULL |
| c_sEL | VARCHAR | YES | NULL |
| NBJAD | INTEGER | YES | NULL |
| EmploiClassificationCCN | VARCHAR | YES | NULL |

#### Exemple de données

| %Salarie       | %CleLBM     |   MATRI | xx   | Date                | ANMOIS   | nomKSL                                       | c_sEL              |   NBJAD | EmploiClassificationCCN   |
|:---------------|:------------|--------:|:-----|:--------------------|:---------|:---------------------------------------------|:-------------------|--------:|:--------------------------|
| BIORYLIS_RH_87 | BIORYLIS_RH |      87 | RYM  | 2022-02-28 00:00:00 | 2022-02  | LA ROCHE-SUR-YON -  BOILEAU LA ROCHE SUR YON | LABORIZON-BIORYLIS |       1 | Personnel de secrétariat  |
| BIORYLIS_RH_87 | BIORYLIS_RH |      87 | RYM  | 2022-06-30 00:00:00 | 2022-06  | LA ROCHE-SUR-YON -  BOILEAU LA ROCHE SUR YON | LABORIZON-BIORYLIS |       1 | Personnel de secrétariat  |
| BIORYLIS_RH_87 | BIORYLIS_RH |      87 | RYM  | 2024-01-31 00:00:00 | 2024-01  | LA ROCHE-SUR-YON -  BOILEAU LA ROCHE SUR YON | LABORIZON-BIORYLIS |       1 | Personnel de secrétariat  |
| BIORYLIS_RH_87 | BIORYLIS_RH |      87 | RYM  | 2024-02-29 00:00:00 | 2024-02  | LA ROCHE-SUR-YON -  BOILEAU LA ROCHE SUR YON | LABORIZON-BIORYLIS |       1 | Personnel de secrétariat  |
| BIORYLIS_RH_87 | BIORYLIS_RH |      87 | RYM  | 2024-03-31 00:00:00 | 2024-03  | LA ROCHE-SUR-YON -  BOILEAU LA ROCHE SUR YON | LABORIZON-BIORYLIS |       1 | Personnel de secrétariat  |

### Table: FAITS_MASSALgpn

**Nombre d'enregistrements:** 210811

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| ANMOIS | VARCHAR | YES | NULL |
| %Salarie | VARCHAR | YES | NULL |
| xx | VARCHAR | YES | NULL |
| SalaireNetAPayer | INTEGER | YES | NULL |
| CoutTotalSalarial | INTEGER | YES | NULL |
| CoutTotalPATSAL | INTEGER | YES | NULL |
| EmploiClassificationCCN | VARCHAR | YES | NULL |

#### Exemple de données

*Données masquées - Table identifiée comme sensible*

### Table: LABOs_enrichi_bassins_de_vie

**Nombre d'enregistrements:** 4671

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| CODE_SITE | VARCHAR | YES | NULL |
| CODE_SITE_ONETREAM | VARCHAR | YES | NULL |
| nomKSL | VARCHAR | YES | NULL |
| geomodifier | VARCHAR | YES | NULL |
| meta.uid | VARCHAR | YES | NULL |
| meta_id | VARCHAR | YES | NULL |
| idYEXT | VARCHAR | YES | NULL |
| finess_ets | VARCHAR | YES | NULL |
| siret | VARCHAR | YES | NULL |
| googlePlaceId | VARCHAR | YES | NULL |
| c_sEL | VARCHAR | YES | NULL |
| name | VARCHAR | YES | NULL |
| c_région | VARCHAR | YES | NULL |
| c_plateauTechnique | VARCHAR | YES | NULL |
| specialities | VARCHAR | YES | NULL |
| closed | VARCHAR | YES | NULL |
| meta.folderId | VARCHAR | YES | NULL |
| Labo 1 | VARCHAR | YES | NULL |
| Labo 2 | VARCHAR | YES | NULL |
| SPD | VARCHAR | YES | NULL |
| autres_lans | VARCHAR | YES | NULL |
| cbmove_ENLEV_nom | VARCHAR | YES | NULL |
| PAYS | VARCHAR | YES | NULL |
| xx | VARCHAR | YES | NULL |
| libelle | VARCHAR | YES | NULL |
| idKSLBLN | VARCHAR | YES | NULL |
| idKSLBMD | VARCHAR | YES | NULL |
| adr1 | VARCHAR | YES | NULL |
| adr3 | VARCHAR | YES | NULL |
| tel | VARCHAR | YES | NULL |
| fax | VARCHAR | YES | NULL |
| hormat | VARCHAR | YES | NULL |
| horapm | VARCHAR | YES | NULL |
| hordiv | VARCHAR | YES | NULL |
| lat | DOUBLE | YES | NULL |
| lon | DOUBLE | YES | NULL |
| lSitesUIfcs | VARCHAR | YES | NULL |
| numlabo_odaUNI | INTEGER | YES | NULL |
| ugeog | VARCHAR | YES | NULL |
| lan | VARCHAR | YES | NULL |
| lanVOIP | VARCHAR | YES | NULL |
| cbODA | INTEGER | YES | NULL |
| refKSL | VARCHAR | YES | NULL |
| sectUNIDYO | VARCHAR | YES | NULL |
| mapSITE | VARCHAR | YES | NULL |
| Référence kalisil | VARCHAR | YES | NULL |
| NOM | VARCHAR | YES | NULL |
| ADRESSE | VARCHAR | YES | NULL |
| CP | VARCHAR | YES | NULL |
| VILLE | VARCHAR | YES | NULL |
| adresse_score | DOUBLE | YES | NULL |
| adresse_id | VARCHAR | YES | NULL |
| adresse_type | VARCHAR | YES | NULL |
| adresse_nom | VARCHAR | YES | NULL |
| code_postal | INTEGER | YES | NULL |
| ville_nom | VARCHAR | YES | NULL |
| citycode | VARCHAR | YES | NULL |
| importance | DOUBLE | YES | NULL |
| BAIL | VARCHAR | YES | NULL |
| SECTEUR | VARCHAR | YES | NULL |
| SECTEUR_LIBELLE | VARCHAR | YES | NULL |
| SECTEUR_CODE | VARCHAR | YES | NULL |
| MINISECTEUR | VARCHAR | YES | NULL |
| MINISECTEUR_LIBELLE | VARCHAR | YES | NULL |
| MINISECTEUR_CODE | VARCHAR | YES | NULL |
| CATSITE | VARCHAR | YES | NULL |
| nomXPLANETDYO | VARCHAR | YES | NULL |
| nomXPLANETMIR | VARCHAR | YES | NULL |
| nomXPLANETORN | VARCHAR | YES | NULL |
| nomXPLANETGLB | VARCHAR | YES | NULL |
| nomKSLDYO | VARCHAR | YES | NULL |
| idKSLDYO | VARCHAR | YES | NULL |
| refKSLDYO | VARCHAR | YES | NULL |
| idKSLMIR | VARCHAR | YES | NULL |
| refKSLMIR | VARCHAR | YES | NULL |
| code_laboCLAMIR | VARCHAR | YES | NULL |
| nomKSLMIR | VARCHAR | YES | NULL |
| idKSLLRB | VARCHAR | YES | NULL |
| refKSLLRB | VARCHAR | YES | NULL |
| claboHXLORN | VARCHAR | YES | NULL |
| idKSLGLB | VARCHAR | YES | NULL |
| refKSLGLB | VARCHAR | YES | NULL |
| nomKSLGLB | VARCHAR | YES | NULL |
| refKSLORN | VARCHAR | YES | NULL |
| idKSLORN | VARCHAR | YES | NULL |
| nomKSLORN | VARCHAR | YES | NULL |
| SAGE_ETS_IDS | VARCHAR | YES | NULL |
| nomKSLLMA | VARCHAR | YES | NULL |
| idKSLLMA | VARCHAR | YES | NULL |
| refKSLLMA | VARCHAR | YES | NULL |
| nomXPLANETSLB | VARCHAR | YES | NULL |
| nomKSLSLB | VARCHAR | YES | NULL |
| idKSLSLB | VARCHAR | YES | NULL |
| refKSLSLB | VARCHAR | YES | NULL |
| nomKSLB86 | VARCHAR | YES | NULL |
| idKSLB86 | VARCHAR | YES | NULL |
| refKSLB86 | VARCHAR | YES | NULL |
| nomKSLA2B | VARCHAR | YES | NULL |
| idKSLA2B | VARCHAR | YES | NULL |
| refKSLA2B | VARCHAR | YES | NULL |
| nomXPLANETBCL | VARCHAR | YES | NULL |
| nomKSLBCL | VARCHAR | YES | NULL |
| idKSLBCL | VARCHAR | YES | NULL |
| refKSLBCL | VARCHAR | YES | NULL |
| nomKSLAST | VARCHAR | YES | NULL |
| idKSLAST | VARCHAR | YES | NULL |
| refKSLAST | VARCHAR | YES | NULL |
| nomXPLANETBMR | VARCHAR | YES | NULL |
| nomKSLBMR | VARCHAR | YES | NULL |
| idKSLBMR | VARCHAR | YES | NULL |
| refKSLBMR | VARCHAR | YES | NULL |
| nomKSLTRL | VARCHAR | YES | NULL |
| idKSLTRL | VARCHAR | YES | NULL |
| refKSLTRL | VARCHAR | YES | NULL |
| idKSLBLT | VARCHAR | YES | NULL |
| refKSLBLT | VARCHAR | YES | NULL |
| nomKSLBMG | VARCHAR | YES | NULL |
| idKSLBMG | VARCHAR | YES | NULL |
| refKSLBMG | VARCHAR | YES | NULL |
| nomKSLMYO | VARCHAR | YES | NULL |
| idKSLMYO | VARCHAR | YES | NULL |
| refKSLMYO | VARCHAR | YES | NULL |
| nomKSLVTA | VARCHAR | YES | NULL |
| idKSLVTA | VARCHAR | YES | NULL |
| refKSLVTA | VARCHAR | YES | NULL |
| nomKSLRYL | VARCHAR | YES | NULL |
| idKSLRYL | VARCHAR | YES | NULL |
| refKSLRYL | VARCHAR | YES | NULL |
| nomKSLLCD | VARCHAR | YES | NULL |
| idKSLLCD | VARCHAR | YES | NULL |
| refKSLLCD | VARCHAR | YES | NULL |
| claboHXLLCD | VARCHAR | YES | NULL |
| idKSLBST | VARCHAR | YES | NULL |
| refKSLBST | VARCHAR | YES | NULL |
| nomKSLBSY | VARCHAR | YES | NULL |
| idKSLBSY | VARCHAR | YES | NULL |
| refKSLBSY | VARCHAR | YES | NULL |
| claboHXLBSY | VARCHAR | YES | NULL |
| nomXPLANETCAB | VARCHAR | YES | NULL |
| nomKSLCAB | VARCHAR | YES | NULL |
| idKSLCAB | VARCHAR | YES | NULL |
| refKSLCAB | VARCHAR | YES | NULL |
| nomXPLANETEMR | VARCHAR | YES | NULL |
| nomKSLEMR | VARCHAR | YES | NULL |
| idKSLEMR | VARCHAR | YES | NULL |
| refKSLEMR | VARCHAR | YES | NULL |
| nomKSLBPO | VARCHAR | YES | NULL |
| idKSLBPO | VARCHAR | YES | NULL |
| refKSLBPO | VARCHAR | YES | NULL |
| claboHXLBPO | VARCHAR | YES | NULL |
| nomXPLANETMYS | VARCHAR | YES | NULL |
| claboHXLMYS | INTEGER | YES | NULL |
| idKSLMYS | INTEGER | YES | NULL |
| refKSLMYS | INTEGER | YES | NULL |
| nomKSLCBM | VARCHAR | YES | NULL |
| idKSLCBM | VARCHAR | YES | NULL |
| refKSLCBM | VARCHAR | YES | NULL |
| nomKSLGNV | VARCHAR | YES | NULL |
| idKSLGNV | VARCHAR | YES | NULL |
| refKSLGNV | VARCHAR | YES | NULL |
| nomKSLLZB | VARCHAR | YES | NULL |
| idKSLLZB | VARCHAR | YES | NULL |
| refKSLLZB | VARCHAR | YES | NULL |
| idKSLLZC | VARCHAR | YES | NULL |
| refKSLLZC | VARCHAR | YES | NULL |
| nomKSLLZC | VARCHAR | YES | NULL |
| idKSLRSB | VARCHAR | YES | NULL |
| refKSLRSB | VARCHAR | YES | NULL |
| nomKSLSMB | VARCHAR | YES | NULL |
| idKSLSMB | VARCHAR | YES | NULL |
| refKSLSMB | VARCHAR | YES | NULL |
| code_laboCLALZC | VARCHAR | YES | NULL |
| code_laboCLALZM | VARCHAR | YES | NULL |
| nomKSLLZM | INTEGER | YES | NULL |
| claboHXLAST | VARCHAR | YES | NULL |
| numlabo_odaAPH | INTEGER | YES | NULL |
| numlabo_ifcAPH | VARCHAR | YES | NULL |
| idKSLAPH | INTEGER | YES | NULL |
| refKSLAPH | INTEGER | YES | NULL |
| nomKSLOME | VARCHAR | YES | NULL |
| idKSLOME | VARCHAR | YES | NULL |
| refKSLOME | VARCHAR | YES | NULL |
| nomKSLKLM | INTEGER | YES | NULL |
| idKSLKLM | INTEGER | YES | NULL |
| refKSLKLM | INTEGER | YES | NULL |
| idKSLBAZ | VARCHAR | YES | NULL |
| nomKSLBAZ | VARCHAR | YES | NULL |
| refKSLBAZ | VARCHAR | YES | NULL |
| numlabo_odaBMR | INTEGER | YES | NULL |
| numlabo_ifcBMR | VARCHAR | YES | NULL |
| refKSLC78 | INTEGER | YES | NULL |
| claboHXLC78 | INTEGER | YES | NULL |
| numlabo_ifcMIR | VARCHAR | YES | NULL |
| Perimetre | VARCHAR | YES | NULL |
| nofinesset | VARCHAR | YES | NULL |
| nofinessej | VARCHAR | YES | NULL |
| rs | VARCHAR | YES | NULL |
| rslongue | VARCHAR | YES | NULL |
| complrs | VARCHAR | YES | NULL |
| compldistrib | VARCHAR | YES | NULL |
| numvoie | VARCHAR | YES | NULL |
| typvoie | VARCHAR | YES | NULL |
| voie | VARCHAR | YES | NULL |
| compvoie | VARCHAR | YES | NULL |
| lieuditbp | VARCHAR | YES | NULL |
| commune | INTEGER | YES | NULL |
| departement | VARCHAR | YES | NULL |
| libdepartement | VARCHAR | YES | NULL |
| ligneacheminement | VARCHAR | YES | NULL |
| telephone | INTEGER | YES | NULL |
| telecopie | INTEGER | YES | NULL |
| categetab | VARCHAR | YES | NULL |
| libcategetab | VARCHAR | YES | NULL |
| categagretab | INTEGER | YES | NULL |
| libcategagretab | VARCHAR | YES | NULL |
| SIRET2 | VARCHAR | YES | NULL |
| codeape | VARCHAR | YES | NULL |
| codemft | INTEGER | YES | NULL |
| libmft | VARCHAR | YES | NULL |
| codesph | INTEGER | YES | NULL |
| libsph | INTEGER | YES | NULL |
| dateouv | VARCHAR | YES | NULL |
| dateautor | VARCHAR | YES | NULL |
| maj | VARCHAR | YES | NULL |
| numuai | INTEGER | YES | NULL |
| coordxet | DOUBLE | YES | NULL |
| coordyet | DOUBLE | YES | NULL |
| sourcecoordet | VARCHAR | YES | NULL |
| datemaj | VARCHAR | YES | NULL |
| Longitude | DOUBLE | YES | NULL |
| Latitude | DOUBLE | YES | NULL |
| EJ | VARCHAR | YES | NULL |
| EJ_SIREN | INTEGER | YES | NULL |
| LBM | VARCHAR | YES | NULL |
| LBMb | VARCHAR | YES | NULL |
| LIBGEO | VARCHAR | YES | NULL |
| BV2022 | VARCHAR | YES | NULL |
| LIBBV2022_x | VARCHAR | YES | NULL |
| TYPE_COMMUNE_BV2022 | INTEGER | YES | NULL |
| DEP | VARCHAR | YES | NULL |
| REG | INTEGER | YES | NULL |
| LIBBV2022_y | VARCHAR | YES | NULL |
| population_2020 | VARCHAR | YES | NULL |
| nb_medecins_2021 | INTEGER | YES | NULL |
| nb_infirmiers_2021 | INTEGER | YES | NULL |
| nb_pharmacies_2021 | INTEGER | YES | NULL |
| part_admin_sante_2020 | VARCHAR | YES | NULL |
| TYPE_BASSIN_VIE | VARCHAR | YES | NULL |

#### Exemple de données

| CODE_SITE   | CODE_SITE_ONETREAM   | nomKSL                                   | geomodifier                              | meta.uid   | meta_id    | idYEXT     | finess_ets   | siret            | googlePlaceId   | c_sEL               | name                                     | c_région   | c_plateauTechnique   | specialities   | closed   | meta.folderId   | Labo 1   | Labo 2   | SPD   | autres_lans   | cbmove_ENLEV_nom   | PAYS   | xx   | libelle                                  | idKSLBLN   | idKSLBMD   | adr1                   | adr3             | tel   | fax   | hormat                               | horapm   | hordiv                               |     lat |     lon | lSitesUIfcs   |   numlabo_odaUNI | ugeog   | lan   | lanVOIP   |   cbODA | refKSL   | sectUNIDYO   | mapSITE   | Référence kalisil   | NOM                                      | ADRESSE                |   CP | VILLE       |   adresse_score | adresse_id   | adresse_type   | adresse_nom   |   code_postal | ville_nom   |   citycode |   importance | BAIL   | SECTEUR   | SECTEUR_LIBELLE   | SECTEUR_CODE   | MINISECTEUR   | MINISECTEUR_LIBELLE   | MINISECTEUR_CODE   | CATSITE             | nomXPLANETDYO   | nomXPLANETMIR   | nomXPLANETORN   | nomXPLANETGLB   | nomKSLDYO   | idKSLDYO   | refKSLDYO   | idKSLMIR   | refKSLMIR   | code_laboCLAMIR   | nomKSLMIR   | idKSLLRB   | refKSLLRB   | claboHXLORN   | idKSLGLB   | refKSLGLB   | nomKSLGLB   | refKSLORN   | idKSLORN   | nomKSLORN   | SAGE_ETS_IDS   | nomKSLLMA   | idKSLLMA   | refKSLLMA   | nomXPLANETSLB   | nomKSLSLB   | idKSLSLB   | refKSLSLB   | nomKSLB86   | idKSLB86   | refKSLB86   | nomKSLA2B   | idKSLA2B   | refKSLA2B   | nomXPLANETBCL   | nomKSLBCL   | idKSLBCL   | refKSLBCL   | nomKSLAST   | idKSLAST   | refKSLAST   | nomXPLANETBMR   | nomKSLBMR   | idKSLBMR   | refKSLBMR   | nomKSLTRL   | idKSLTRL   | refKSLTRL   | idKSLBLT   | refKSLBLT   | nomKSLBMG   | idKSLBMG   | refKSLBMG   | nomKSLMYO   | idKSLMYO   | refKSLMYO   | nomKSLVTA   | idKSLVTA   | refKSLVTA   | nomKSLRYL   | idKSLRYL   | refKSLRYL   | nomKSLLCD   | idKSLLCD   | refKSLLCD   | claboHXLLCD   | idKSLBST   | refKSLBST   | nomKSLBSY   | idKSLBSY   | refKSLBSY   | claboHXLBSY   | nomXPLANETCAB   | nomKSLCAB   | idKSLCAB   | refKSLCAB   | nomXPLANETEMR   | nomKSLEMR   | idKSLEMR   | refKSLEMR   | nomKSLBPO   | idKSLBPO   | refKSLBPO   | claboHXLBPO   | nomXPLANETMYS   |   claboHXLMYS |   idKSLMYS |   refKSLMYS | nomKSLCBM   | idKSLCBM   | refKSLCBM   | nomKSLGNV   | idKSLGNV   | refKSLGNV   | nomKSLLZB   | idKSLLZB   | refKSLLZB   | idKSLLZC   | refKSLLZC   | nomKSLLZC   | idKSLRSB   | refKSLRSB   | nomKSLSMB   | idKSLSMB   | refKSLSMB   | code_laboCLALZC   | code_laboCLALZM   |   nomKSLLZM | claboHXLAST   |   numlabo_odaAPH | numlabo_ifcAPH   |   idKSLAPH |   refKSLAPH | nomKSLOME   | idKSLOME   | refKSLOME   |   nomKSLKLM |   idKSLKLM |   refKSLKLM | idKSLBAZ   | nomKSLBAZ   | refKSLBAZ   |   numlabo_odaBMR | numlabo_ifcBMR   |   refKSLC78 |   claboHXLC78 | numlabo_ifcMIR   | Perimetre   | nofinesset   | nofinessej   | rs   | rslongue   | complrs   | compldistrib   | numvoie   | typvoie   | voie   | compvoie   | lieuditbp   |   commune | departement   | libdepartement   | ligneacheminement   |   telephone |   telecopie | categetab   | libcategetab   |   categagretab | libcategagretab   | SIRET2   | codeape   |   codemft | libmft   |   codesph |   libsph | dateouv   | dateautor   | maj   |   numuai |   coordxet |   coordyet | sourcecoordet   | datemaj   |   Longitude |   Latitude | EJ   |   EJ_SIREN | LBM   | LBMb   | LIBGEO   | BV2022   | LIBBV2022_x   |   TYPE_COMMUNE_BV2022 | DEP   |   REG | LIBBV2022_y   | population_2020   |   nb_medecins_2021 |   nb_infirmiers_2021 |   nb_pharmacies_2021 | part_admin_sante_2020   | TYPE_BASSIN_VIE   |
|:------------|:---------------------|:-----------------------------------------|:-----------------------------------------|:-----------|:-----------|:-----------|:-------------|:-----------------|:----------------|:--------------------|:-----------------------------------------|:-----------|:---------------------|:---------------|:---------|:----------------|:---------|:---------|:------|:--------------|:-------------------|:-------|:-----|:-----------------------------------------|:-----------|:-----------|:-----------------------|:-----------------|:------|:------|:-------------------------------------|:---------|:-------------------------------------|--------:|--------:|:--------------|-----------------:|:--------|:------|:----------|--------:|:---------|:-------------|:----------|:--------------------|:-----------------------------------------|:-----------------------|-----:|:------------|----------------:|:-------------|:---------------|:--------------|--------------:|:------------|-----------:|-------------:|:-------|:----------|:------------------|:---------------|:--------------|:----------------------|:-------------------|:--------------------|:----------------|:----------------|:----------------|:----------------|:------------|:-----------|:------------|:-----------|:------------|:------------------|:------------|:-----------|:------------|:--------------|:-----------|:------------|:------------|:------------|:-----------|:------------|:---------------|:------------|:-----------|:------------|:----------------|:------------|:-----------|:------------|:------------|:-----------|:------------|:------------|:-----------|:------------|:----------------|:------------|:-----------|:------------|:------------|:-----------|:------------|:----------------|:------------|:-----------|:------------|:------------|:-----------|:------------|:-----------|:------------|:------------|:-----------|:------------|:------------|:-----------|:------------|:------------|:-----------|:------------|:------------|:-----------|:------------|:------------|:-----------|:------------|:--------------|:-----------|:------------|:------------|:-----------|:------------|:--------------|:----------------|:------------|:-----------|:------------|:----------------|:------------|:-----------|:------------|:------------|:-----------|:------------|:--------------|:----------------|--------------:|-----------:|------------:|:------------|:-----------|:------------|:------------|:-----------|:------------|:------------|:-----------|:------------|:-----------|:------------|:------------|:-----------|:------------|:------------|:-----------|:------------|:------------------|:------------------|------------:|:--------------|-----------------:|:-----------------|-----------:|------------:|:------------|:-----------|:------------|------------:|-----------:|------------:|:-----------|:------------|:------------|-----------------:|:-----------------|------------:|--------------:|:-----------------|:------------|:-------------|:-------------|:-----|:-----------|:----------|:---------------|:----------|:----------|:-------|:-----------|:------------|----------:|:--------------|:-----------------|:--------------------|------------:|------------:|:------------|:---------------|---------------:|:------------------|:---------|:----------|----------:|:---------|----------:|---------:|:----------|:------------|:------|---------:|-----------:|-----------:|:----------------|:----------|------------:|-----------:|:-----|-----------:|:------|:-------|:---------|:---------|:--------------|----------------------:|:------|------:|:--------------|:------------------|-------------------:|---------------------:|---------------------:|:------------------------|:------------------|
| S0740_13    | NULL                 | LABORATOIRES REUNIS - Colmar-Berg - 7740 | LABORATOIRES REUNIS - Colmar-Berg - 7740 | qbrGbd     | COB        | COB        | NULL         | Périmètre actuel | NULL            | LABORATOIRES REUNIS | LABORATOIRES REUNIS - Colmar-Berg - 7740 | Luxembourg | NULL                 | NULL           | LUX      | NULL            | NULL     | NULL     | S     | NULL          | NULL               | LUX    | LVM  | LABORATOIRES REUNIS - Colmar-Berg - 7740 | NULL       | NULL       | 4b Avenue Gordon Smith | 7740 Colmar-Berg | NULL  | NULL  | [{'start': '07:00', 'end': '11:00'}] | NULL     | NULL                                 | 49.8112 | 6.09631 | NULL          |              nan | NULL    | NULL  | NULL      |     nan | NULL     | NULL         | NULL      | NULL                | LABORATOIRES REUNIS - Colmar-Berg - 7740 | 4b Avenue Gordon Smith | 7740 | Colmar-Berg |             nan | NULL         | NULL           | NULL          |           nan | NULL        |        nan |          nan | NULL   | LR_LUX    | NULL              | NULL           | NULL          | NULL                  | NULL               | LABORATOIRES REUNIS | NULL            | NULL            | NULL            | NULL            | NULL        | NULL       | NULL        | NULL       | NULL        | NULL              | NULL        | NULL       | NULL        | NULL          | NULL       | NULL        | NULL        | NULL        | NULL       | NULL        | NULL           | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL            | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL            |           nan |        nan |         nan | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL              | NULL              |         nan | NULL          |              nan | NULL             |        nan |         nan | NULL        | NULL       | NULL        |         nan |        nan |         nan | NULL       | NULL        | NULL        |              nan | NULL             |         nan |           nan | NULL             | NULL        | NULL         | NULL         | NULL | NULL       | NULL      | NULL           | NULL      | NULL      | NULL   | NULL       | NULL        |       nan | NULL          | NULL             | NULL                |         nan |         nan | NULL        | NULL           |            nan | NULL              | xxxx     | NULL      |       nan | NULL     |       nan |      nan | NULL      | NULL        | NULL  |      nan |        nan |        nan | NULL            | NULL      |         nan |        nan | NULL |        nan | NULL  | NULL   | NULL     | NULL     | NULL          |                   nan | NULL  |   nan | NULL          | NULL              |                nan |                  nan |                  nan | NULL                    | NULL              |
| S0740_14    | NULL                 | LABORATOIRES REUNIS - Dahlem - 8352      | LABORATOIRES REUNIS - Dahlem - 8352      | wVXNVn     | DAG        | DAG        | NULL         | Périmètre actuel | NULL            | LABORATOIRES REUNIS | LABORATOIRES REUNIS - Dahlem - 8352      | Luxembourg | NULL                 | NULL           | LUX      | NULL            | NULL     | NULL     | S     | NULL          | NULL               | LUX    | LVN  | LABORATOIRES REUNIS - Dahlem - 8352      | NULL       | NULL       | 14 Rue de l'École      | 8352 Dahlem      | NULL  | NULL  | NULL                                 | NULL     | NULL                                 | 49.5996 | 5.94648 | NULL          |              nan | NULL    | NULL  | NULL      |     nan | NULL     | NULL         | NULL      | NULL                | LABORATOIRES REUNIS - Dahlem - 8352      | 14 Rue de l'ecole      | 8352 | Dahlem      |             nan | NULL         | NULL           | NULL          |           nan | NULL        |        nan |          nan | NULL   | LR_LUX    | NULL              | NULL           | NULL          | NULL                  | NULL               | LABORATOIRES REUNIS | NULL            | NULL            | NULL            | NULL            | NULL        | NULL       | NULL        | NULL       | NULL        | NULL              | NULL        | NULL       | NULL        | NULL          | NULL       | NULL        | NULL        | NULL        | NULL       | NULL        | NULL           | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL            | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL            |           nan |        nan |         nan | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL              | NULL              |         nan | NULL          |              nan | NULL             |        nan |         nan | NULL        | NULL       | NULL        |         nan |        nan |         nan | NULL       | NULL        | NULL        |              nan | NULL             |         nan |           nan | NULL             | NULL        | NULL         | NULL         | NULL | NULL       | NULL      | NULL           | NULL      | NULL      | NULL   | NULL       | NULL        |       nan | NULL          | NULL             | NULL                |         nan |         nan | NULL        | NULL           |            nan | NULL              | xxxx     | NULL      |       nan | NULL     |       nan |      nan | NULL      | NULL        | NULL  |      nan |        nan |        nan | NULL            | NULL      |         nan |        nan | NULL |        nan | NULL  | NULL   | NULL     | NULL     | NULL          |                   nan | NULL  |   nan | NULL          | NULL              |                nan |                  nan |                  nan | NULL                    | NULL              |
| S0740_27    | NULL                 | LABORATOIRES REUNIS - Luxembourg - 1278  | LABORATOIRES REUNIS - Luxembourg - 1278  | 9ajyal     | GA         | GA         | NULL         | NULL             | NULL            | LABORATOIRES REUNIS | LABORATOIRES REUNIS - Luxembourg - 1278  | Luxembourg | NULL                 | NULL           | LUX      | NULL            | NULL     | NULL     | S     | NULL          | NULL               | LUX    | LVD  | LABORATOIRES REUNIS - Luxembourg - 1278  | NULL       | NULL       | 4 Rue Tony Bourg       | 1278 Luxembourg  | NULL  | NULL  | [{'start': '07:00', 'end': '11:00'}] | NULL     | NULL                                 | 49.5918 | 6.12518 | NULL          |              nan | NULL    | NULL  | NULL      |     nan | NULL     | NULL         | NULL      | NULL                | LABORATOIRES REUNIS - Luxembourg - 1278  | 4 Rue Tony Bourg       | 1278 | Luxembourg  |             nan | NULL         | NULL           | NULL          |           nan | NULL        |        nan |          nan | NULL   | LR_LUX    | NULL              | NULL           | NULL          | NULL                  | NULL               | LABORATOIRES REUNIS | NULL            | NULL            | NULL            | NULL            | NULL        | NULL       | NULL        | NULL       | NULL        | NULL              | NULL        | NULL       | NULL        | NULL          | NULL       | NULL        | NULL        | NULL        | NULL       | NULL        | NULL           | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL            | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL            |           nan |        nan |         nan | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL              | NULL              |         nan | NULL          |              nan | NULL             |        nan |         nan | NULL        | NULL       | NULL        |         nan |        nan |         nan | NULL       | NULL        | NULL        |              nan | NULL             |         nan |           nan | NULL             | NULL        | NULL         | NULL         | NULL | NULL       | NULL      | NULL           | NULL      | NULL      | NULL   | NULL       | NULL        |       nan | NULL          | NULL             | NULL                |         nan |         nan | NULL        | NULL           |            nan | NULL              | xxxx     | NULL      |       nan | NULL     |       nan |      nan | NULL      | NULL        | NULL  |      nan |        nan |        nan | NULL            | NULL      |         nan |        nan | NULL |        nan | NULL  | NULL   | NULL     | NULL     | NULL          |                   nan | NULL  |   nan | NULL          | NULL              |                nan |                  nan |                  nan | NULL                    | NULL              |
| S0740_59    | NULL                 | LABORATOIRES REUNIS - Remich - 5531      | LABORATOIRES REUNIS - Remich - 5531      | dA4qA4     | cm-re_labo | cm-re_labo | NULL         | NULL             | NULL            | LABORATOIRES REUNIS | LABORATOIRES REUNIS - Remich - 5531      | Luxembourg | NULL                 | NULL           | LUX      | NULL            | NULL     | NULL     | S     | NULL          | NULL               | LUX    | LVO  | LABORATOIRES REUNIS - Remich - 5531      | NULL       | NULL       | 34 Route de l'Europe   | 5531 Remich      | NULL  | NULL  | [{'start': '07:00', 'end': '12:00'}] | NULL     | [{'start': '07:00', 'end': '10:00'}] | 49.5483 | 6.35903 | NULL          |              nan | NULL    | NULL  | NULL      |     nan | NULL     | NULL         | NULL      | NULL                | LABORATOIRES REUNIS - Remich - 5531      | 34 Route de l'Europe   | 5531 | Remich      |             nan | NULL         | NULL           | NULL          |           nan | NULL        |        nan |          nan | NULL   | LR_LUX    | NULL              | NULL           | NULL          | NULL                  | NULL               | LABORATOIRES REUNIS | NULL            | NULL            | NULL            | NULL            | NULL        | NULL       | NULL        | NULL       | NULL        | NULL              | NULL        | NULL       | NULL        | NULL          | NULL       | NULL        | NULL        | NULL        | NULL       | NULL        | NULL           | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL            | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL            |           nan |        nan |         nan | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL              | NULL              |         nan | NULL          |              nan | NULL             |        nan |         nan | NULL        | NULL       | NULL        |         nan |        nan |         nan | NULL       | NULL        | NULL        |              nan | NULL             |         nan |           nan | NULL             | NULL        | NULL         | NULL         | NULL | NULL       | NULL      | NULL           | NULL      | NULL      | NULL   | NULL       | NULL        |       nan | NULL          | NULL             | NULL                |         nan |         nan | NULL        | NULL           |            nan | NULL              | xxxx     | NULL      |       nan | NULL     |       nan |      nan | NULL      | NULL        | NULL  |      nan |        nan |        nan | NULL            | NULL      |         nan |        nan | NULL |        nan | NULL  | NULL   | NULL     | NULL     | NULL          |                   nan | NULL  |   nan | NULL          | NULL              |                nan |                  nan |                  nan | NULL                    | NULL              |
| S0740_36    | NULL                 | LABORATOIRES REUNIS - Bonnevoie - 1133   | LABORATOIRES REUNIS - Bonnevoie - 1133   | nmyGmD     | BO_26      | BO_26      | NULL         | NULL             | NULL            | LABORATOIRES REUNIS | LABORATOIRES REUNIS - Bonnevoie - 1133   | Luxembourg | NULL                 | NULL           | LUX      | NULL            | NULL     | NULL     | S     | NULL          | NULL               | LUX    | LVR  | LABORATOIRES REUNIS - Bonnevoie - 1133   | NULL       | NULL       | 22 Rue des Ardennes    | 1133 Bonnevoie   | NULL  | NULL  | [{'start': '06:30', 'end': '11:00'}] | NULL     | [{'start': '06:30', 'end': '11:00'}] | 49.5969 | 6.13746 | NULL          |              nan | NULL    | NULL  | NULL      |     nan | NULL     | NULL         | NULL      | NULL                | LABORATOIRES REUNIS - Bonnevoie - 1133   | 22 Rue des Ardennes    | 1133 | Bonnevoie   |             nan | NULL         | NULL           | NULL          |           nan | NULL        |        nan |          nan | NULL   | LR_LUX    | NULL              | NULL           | NULL          | NULL                  | NULL               | LABORATOIRES REUNIS | NULL            | NULL            | NULL            | NULL            | NULL        | NULL       | NULL        | NULL       | NULL        | NULL              | NULL        | NULL       | NULL        | NULL          | NULL       | NULL        | NULL        | NULL        | NULL       | NULL        | NULL           | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL            | NULL        | NULL       | NULL        | NULL            | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL          | NULL            |           nan |        nan |         nan | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL        | NULL       | NULL        | NULL              | NULL              |         nan | NULL          |              nan | NULL             |        nan |         nan | NULL        | NULL       | NULL        |         nan |        nan |         nan | NULL       | NULL        | NULL        |              nan | NULL             |         nan |           nan | NULL             | NULL        | NULL         | NULL         | NULL | NULL       | NULL      | NULL           | NULL      | NULL      | NULL   | NULL       | NULL        |       nan | NULL          | NULL             | NULL                |         nan |         nan | NULL        | NULL           |            nan | NULL              | xxxx     | NULL      |       nan | NULL     |       nan |      nan | NULL      | NULL        | NULL  |      nan |        nan |        nan | NULL            | NULL      |         nan |        nan | NULL |        nan | NULL  | NULL   | NULL     | NULL     | NULL          |                   nan | NULL  |   nan | NULL          | NULL              |                nan |                  nan |                  nan | NULL                    | NULL              |

### Table: REFERENTIEL_SITE_ERP

**Nombre d'enregistrements:** 1304

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| Région Biogroup[JROS] | VARCHAR | YES | NULL |
| Code entité[JROS] | VARCHAR | YES | NULL |
| Libellé entité[JROS] | VARCHAR | YES | NULL |
| Code Site (Name OS)[JROS] | VARCHAR | YES | NULL |
| Type de site[JROS] | VARCHAR | YES | NULL |
| Libellé Site (Description site OS)[JROS] | VARCHAR | YES | NULL |
| Adresse[JROS] | VARCHAR | YES | NULL |
| Code postal[JROS] | VARCHAR | YES | NULL |
| Ville[JROS] | VARCHAR | YES | NULL |
| finess_ets[LTLABOs] | VARCHAR | YES | NULL |
| SIRET[JROS] | VARCHAR | YES | NULL |
| TRIGRAM[LTLABOs] | VARCHAR | YES | NULL |
| ETABLI[LTLABOs] | VARCHAR | YES | NULL |
| EQUIPES[HR4U] | VARCHAR | YES | NULL |
| IDJUR[RDD] | VARCHAR | YES | NULL |

#### Exemple de données

| Région Biogroup[JROS]   | Code entité[JROS]   | Libellé entité[JROS]   | Code Site (Name OS)[JROS]   | Type de site[JROS]   | Libellé Site (Description site OS)[JROS]   | Adresse[JROS]                |   Code postal[JROS] | Ville[JROS]          | finess_ets[LTLABOs]   |    SIRET[JROS] | TRIGRAM[LTLABOs]   | ETABLI[LTLABOs]   | EQUIPES[HR4U]                                                                                                                                                                                                                                    | IDJUR[RDD]   |
|:------------------------|:--------------------|:-----------------------|:----------------------------|:---------------------|:-------------------------------------------|:-----------------------------|--------------------:|:---------------------|:----------------------|---------------:|:-------------------|:------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------|
| Général                 | E0010               | BIOGROUP SCM           | S0010_04                    | SIEGE                | Boulogne Billancourt                       | 1 rue Edighoffen             |               68000 | COLMAR               | NULL                  | 38107442600010 | NULL               | NULL              | NULL                                                                                                                                                                                                                                             | NULL         |
| Général                 | E0010               | BIOGROUP SCM           | S0010_01                    | SIEGE                | Colmar - Avenue d'Alésia                   | 203 avenue d'Alsace          |               68000 | COLMAR               | NULL                  | 38107442600028 | SCC                | BG001             | Eq Architecture¦Eq CISO office¦Eq Communication¦Eq Comptabilité¦Eq Data & Analytics¦Eq Dir Technique IT¦Eq Juridique¦Eq Paye¦Eq Qualité¦Eq RH¦Eq SSE-RSE¦Eq Services Généraux¦Eq Solutions IT Business¦Eq Solutions IT Labos¦Eq Transfo Digitale | NULL         |
| Général                 | E0010               | BIOGROUP SCM           | S0010_05                    | SIEGE                | Colmar - Rue Edighoffen                    | 70 boulevard Anatole France  |               93200 | SAINT-DENIS          | NULL                  | 38107442600036 | NULL               | NULL              | NULL                                                                                                                                                                                                                                             | NULL         |
| Général                 | E0010               | BIOGROUP SCM           | S0010_06                    | SIEGE                | Saint Denis                                | 88 avenue du Général Leclerc |               92100 | BOULOGNE-BILLANCOURT | NULL                  | 38107442600044 | NULL               | NULL              | NULL                                                                                                                                                                                                                                             | NULL         |
| Général                 | E0010               | BIOGROUP SCM           | S0010_02                    | SIEGE                | MOUANS-SARTOUX                             | 460 avenue de la Quiéra      |               06370 | MOUANS-SARTOUX       | 38107442600051        | 38107442600051 | SCA                | BG002             | Eq Architecture¦Eq CISO office¦Eq Communication¦Eq Data & Analytics¦Eq Dir Technique IT¦Eq Qualité¦Eq Recherche et Innov¦Eq Services Généraux¦Eq Solutions IT Business¦Eq Solutions IT Labos¦Eq Transfo Digitale                                 | NULL         |

### Table: csvreport_sfa_all

**Nombre d'enregistrements:** Erreur

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| nomksl_dbtm | VARCHAR | YES | NULL |
| xx_dbtm | VARCHAR | YES | NULL |
| demandedate | DATE | YES | NULL |
| ca | DOUBLE | YES | NULL |

### Table: duplicates_SILs_idCorrespondants

**Nombre d'enregistrements:** 2

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| SILs_idCorrespondants | VARCHAR | YES | NULL |
| Type ETS | VARCHAR | YES | NULL |
| Ville | VARCHAR | YES | NULL |

#### Exemple de données

| SILs_idCorrespondants   | Type ETS   | Ville      |
|:------------------------|:-----------|:-----------|
| ksldyo_635              | EHPAD      | 69009 LYON |
| ksldyo_635              | FAM        | LYON       |

### Table: id_pharmacie_mapping

**Nombre d'enregistrements:** 11

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| id | VARCHAR | YES | NULL |
| type | VARCHAR | YES | NULL |

#### Exemple de données

| id            | type      |
|:--------------|:----------|
| ¦ksldyo_592¦  | Pharmacie |
| ¦kslast_6880¦ | Pharmacie |
| ¦ksldyo_2294¦ | Pharmacie |
| ¦ksldyo_2289¦ | Pharmacie |
| ¦ksldyo_9361¦ | Pharmacie |

### Table: labos

**Nombre d'enregistrements:** 4671

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| xx | VARCHAR | YES | NULL |
| nomKSL | VARCHAR | YES | NULL |
| c_sEL | VARCHAR | YES | NULL |

#### Exemple de données

| xx   | nomKSL                                   | c_sEL               |
|:-----|:-----------------------------------------|:--------------------|
| LVM  | LABORATOIRES REUNIS - Colmar-Berg - 7740 | LABORATOIRES REUNIS |
| LVN  | LABORATOIRES REUNIS - Dahlem - 8352      | LABORATOIRES REUNIS |
| LVD  | LABORATOIRES REUNIS - Luxembourg - 1278  | LABORATOIRES REUNIS |
| LVO  | LABORATOIRES REUNIS - Remich - 5531      | LABORATOIRES REUNIS |
| LVR  | LABORATOIRES REUNIS - Bonnevoie - 1133   | LABORATOIRES REUNIS |

### Table: mapping_SILs_idCorrespondants_cps

**Nombre d'enregistrements:** 2430

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| sil_idcorrespondant | VARCHAR | YES | NULL |
| type_cps | VARCHAR | YES | NULL |

#### Exemple de données

| sil_idcorrespondant   | type_cps   |
|:----------------------|:-----------|
| kslsmb_5234           | Pharmacie  |
| kslcab_14506          | Divers     |
| ksltrl_16822          | Pharmacie  |
| kslemr_4577           | Pharmacie  |
| kslcab_12084          | Pharmacie  |

### Table: mapping_SILs_idPersonnels_rhs

**Nombre d'enregistrements:** 37224

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| SILs_idPersonnels | VARCHAR | YES | NULL |
| nss | VARCHAR | YES | NULL |

#### Exemple de données

| SILs_idPersonnels   | nss    |
|:--------------------|:-------|
| ksla2b_133          | ****** |
| ksla2b_69           | ****** |
| ksla2b_75           | ****** |
| ksla2b_124          | ****** |
| ksla2b_5            | ****** |

### Table: mapping_idsite_labos

**Nombre d'enregistrements:** 1919

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| group | VARCHAR | YES | NULL |
| idsite | VARCHAR | YES | NULL |
| row_index | INTEGER | YES | NULL |
| nomKSL | VARCHAR | YES | NULL |
| SECTEUR | VARCHAR | YES | NULL |
| MINISECTEUR | VARCHAR | YES | NULL |
| xx | VARCHAR | YES | NULL |
| c_sEL | VARCHAR | YES | NULL |
| finess_ets | VARCHAR | YES | NULL |
| CP | VARCHAR | YES | NULL |
| REGION | VARCHAR | YES | NULL |
| citycode | VARCHAR | YES | NULL |
| TYPE_BASSIN_VIE | VARCHAR | YES | NULL |

#### Exemple de données

| group    | idsite      |   row_index | nomKSL                                                       | SECTEUR      | MINISECTEUR                | xx         | c_sEL              | finess_ets   |    CP | REGION   |   citycode | TYPE_BASSIN_VIE                 |
|:---------|:------------|------------:|:-------------------------------------------------------------|:-------------|:---------------------------|:-----------|:-------------------|:-------------|------:|:---------|-----------:|:--------------------------------|
| idKSL    | ksllzb_0030 |          19 | BIOGROUP - Plateau Technique de Noyal                        | PT           | PT                         | LIA        | LABORIZON-BRETAGNE | 350054631    | 35230 | GdOUEST  |      35206 | Urbain dense                    |
| idKSL    | kslorn_0012 |          21 | ST MARTIN D'HERES BELLEDONNE(OPTIMED)                        | GRENOBLE EST | Abbaye + Belledonne + Péri | OHBoptimed | ORIADE-NOVIALE     | NULL         | 38400 | AURA     |      38421 | Urbain dense                    |
| claboHXL | hxlorn_o    |          21 | ST MARTIN D'HERES BELLEDONNE(OPTIMED)                        | GRENOBLE EST | Abbaye + Belledonne + Péri | OHBoptimed | ORIADE-NOVIALE     | NULL         | 38400 | AURA     |      38421 | Urbain dense                    |
| idKSL    | ksllzb_0049 |          23 | BIOGROUP LABORIZON BRETAGNE - Plateau technique de Carquefou | PT           | PT                         | LIB        | LABORIZON-BRETAGNE | 440059772    | 44470 | GdOUEST  |      44026 | Urbain dense                    |
| idKSL    | kslmir_0040 |          25 | Mirialis-Bechet-PT                                           | PT           | PT                         | CLW        | MIRIALIS           | NULL         | 74300 | AURA     |      74081 | Urbain de densité intermédiaire |

### Table: mapping_pcod_rhs

**Nombre d'enregistrements:** 37801

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| pcod | VARCHAR | YES | NULL |
| nss | VARCHAR | YES | NULL |

#### Exemple de données

| pcod        | nss    |
|:------------|:-------|
| ksla2b_p352 | ****** |
| ksla2b_p681 | ****** |
| ksla2b_p10  | ****** |
| ksla2b_p499 | ****** |
| ksla2b_p351 | ****** |

### Table: mapping_xplans_codes_rhs

**Nombre d'enregistrements:** 14000

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| xplans_codes | VARCHAR | YES | NULL |
| nss | VARCHAR | YES | NULL |

#### Exemple de données

| xplans_codes   | nss    |
|:---------------|:-------|
| xpbmr_HAAB     | ****** |
| xpbmr_ABKH     | ****** |
| xpbmr_ABDHI    | ****** |
| xpbmr_ACMO     | ****** |
| xpbmr_EA       | ****** |

### Table: metricflow_time_spine

**Nombre d'enregistrements:** 4017

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| date_day | TIMESTAMP | YES | NULL |

#### Exemple de données

| date_day            |
|:--------------------|
| 2020-01-01 00:00:00 |
| 2020-01-02 00:00:00 |
| 2020-01-03 00:00:00 |
| 2020-01-04 00:00:00 |
| 2020-01-05 00:00:00 |

### Table: sildemanderesultat

**Nombre d'enregistrements:** 191308463

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| idResultat | VARCHAR | YES | NULL |

#### Exemple de données

|   idResultat |
|-------------:|
|  1.47804e+07 |
|  1.45839e+07 |
|  1.83742e+07 |
|  2.31449e+07 |
|  2.31449e+07 |

### Table: silpatient

**Nombre d'enregistrements:** 5228887

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| saisiedate | DATE | YES | NULL |
| silsrc | VARCHAR | YES | NULL |
| idpatient | VARCHAR | YES | NULL |
| phone | VARCHAR | YES | NULL |
| nss13 | VARCHAR | YES | NULL |
| identifier | VARCHAR | YES | NULL |
| email | VARCHAR | YES | NULL |

#### Exemple de données

| saisiedate          | silsrc   | idpatient   | phone      | nss13   | identifier                           | email                      |
|:--------------------|:---------|:------------|:-----------|:--------|:-------------------------------------|:---------------------------|
| 2019-11-29 00:00:00 | ksla2b   | ksla2b_pat1 | NULL       | NULL    | ID1¦M1981¦ID1¦ESSAINETIKA¦1981-01-17 | NULL                       |
| 2019-12-03 00:00:00 | ksla2b   | ksla2b_pat2 | 0686708875 | NULL    | ID2¦F2022¦ID2¦TESTMAGALI¦2022-01-01  | m.hypolite@biogroup-lcd.fr |
| 2019-12-05 00:00:00 | ksla2b   | ksla2b_pat3 | NULL       | NULL    | ID3¦F2001¦ID3¦ESSAIMYLA¦2001-12-05   | NULL                       |
| 2019-12-05 00:00:00 | ksla2b   | ksla2b_pat4 | NULL       | NULL    | ID4¦F1992¦ID4¦ESSAIVIDAS¦1992-03-08  | NULL                       |
| 2019-12-05 00:00:00 | ksla2b   | ksla2b_pat5 | NULL       | NULL    | ID5¦M2000¦ID5¦ESSAIMYLA¦2000-12-05   | NULL                       |

### Table: stg_sfa_monthly

**Nombre d'enregistrements:** 273111274

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| an | BIGINT | YES | NULL |
| mois | BIGINT | YES | NULL |
| Age | INTEGER | YES | NULL |
| TH | VARCHAR | YES | NULL |
| nss | VARCHAR | YES | NULL |
| idpatient | VARCHAR | YES | NULL |
| numdemande | VARCHAR | YES | NULL |
| iddemande | VARCHAR | YES | NULL |
| demande_gpskey | VARCHAR | YES | NULL |
| medid | VARCHAR | YES | NULL |
| sexe | VARCHAR | YES | NULL |
| actrtot | DECIMAL(10,2) | YES | NULL |
| mntprel | DOUBLE | YES | NULL |
| mntactes | VARCHAR | YES | NULL |
| silsrc | VARCHAR | YES | NULL |
| cnomencgc | VARCHAR | YES | NULL |
| saisie_par | VARCHAR | YES | NULL |
| ccor_ids | VARCHAR | YES | NULL |
| demandedate | DATE | YES | NULL |
| validedate | DATE | YES | NULL |
| prel_par | VARCHAR | YES | NULL |
| idsite | VARCHAR | YES | NULL |
| sdem_idSite | VARCHAR | YES | NULL |
| z_dom_ext | VARCHAR | YES | NULL |
| qvd_mtime | TIMESTAMP WITH TIME ZONE | YES | NULL |
| pqt_mtime | TIMESTAMP WITH TIME ZONE | YES | NULL |
| mntcai | DOUBLE | YES | NULL |
| mntmut | DOUBLE | YES | NULL |
| mntpat | DOUBLE | YES | NULL |
| mntcor | DOUBLE | YES | NULL |
| remcor | DECIMAL(11,2) | YES | NULL |
| tbv_dbtm | VARCHAR | YES | NULL |
| region_dbtm | VARCHAR | YES | NULL |
| secteur_dbtm | VARCHAR | YES | NULL |
| xx_dbtm | VARCHAR | YES | NULL |
| nomksl_dbtm | VARCHAR | YES | NULL |
| saisie_par_nss_dbtm | VARCHAR | YES | NULL |
| prel_par_nss_dbtm | VARCHAR | YES | NULL |
| pharmacieg | VARCHAR | YES | NULL |
| ehpadg | VARCHAR | YES | NULL |
| cliniqueg | VARCHAR | YES | NULL |
| latitude | VARCHAR | YES | NULL |
| longitude | VARCHAR | YES | NULL |
| result_citycode | VARCHAR | YES | NULL |
| cairgm | VARCHAR | YES | NULL |
| caidpt | VARCHAR | YES | NULL |
| caicod | VARCHAR | YES | NULL |
| mutcod | VARCHAR | YES | NULL |
| sdem_ordonnanceDateISO | TIMESTAMP_NS | YES | NULL |
| delai_demande_ordo | DOUBLE | YES | NULL |
| delai_saisie_validation | DOUBLE | YES | NULL |
| PQLG | VARCHAR | YES | NULL |
| preleveDatetimeISOG | TIMESTAMP_NS | YES | NULL |
| preleveParG | VARCHAR | YES | NULL |
| receptionDatetimeISOG | TIMESTAMP_NS | YES | NULL |
| delai_prelevement_receptionG | DOUBLE | YES | NULL |
| updated_at | TIMESTAMP WITH TIME ZONE | YES | NULL |

#### Exemple de données

|   an |   mois |   Age |   TH | nss    | idpatient        | numdemande        | iddemande     | demande_gpskey   | medid         | sexe   |   actrtot |   mntprel |   mntactes | silsrc   | cnomencgc                                                                    | saisie_par   | ccor_ids   | demandedate         | validedate          | prel_par    | idsite          | sdem_idSite     | z_dom_ext   | qvd_mtime                        | pqt_mtime                        |   mntcai |   mntmut |   mntpat |   mntcor |   remcor | tbv_dbtm                        | region_dbtm   | secteur_dbtm   | xx_dbtm   | nomksl_dbtm   | saisie_par_nss_dbtm   | prel_par_nss_dbtm   | pharmacieg   | ehpadg   | cliniqueg   |   latitude |   longitude |   result_citycode | cairgm   | caidpt   | caicod   | mutcod   | sdem_ordonnanceDateISO   |   delai_demande_ordo |   delai_saisie_validation | PQLG   | preleveDatetimeISOG   | preleveParG   | receptionDatetimeISOG   |   delai_prelevement_receptionG | updated_at                       |
|-----:|-------:|------:|-----:|:-------|:-----------------|:------------------|:--------------|:-----------------|:--------------|:-------|----------:|----------:|-----------:|:---------|:-----------------------------------------------------------------------------|:-------------|:-----------|:--------------------|:--------------------|:------------|:----------------|:----------------|:------------|:---------------------------------|:---------------------------------|---------:|---------:|---------:|---------:|---------:|:--------------------------------|:--------------|:---------------|:----------|:--------------|:----------------------|:--------------------|:-------------|:---------|:------------|-----------:|------------:|------------------:|:---------|:---------|:---------|:---------|:-------------------------|---------------------:|--------------------------:|:-------|:----------------------|:--------------|:------------------------|-------------------------------:|:---------------------------------|
| 2012 |      4 |    67 |   08 | ****** | clamir_pat665246 | clamir_2000008413 | clamir_384458 | clamir_384458    | clamir_POTYHE | M      |     33.75 |      4.89 |      28.86 | clamir   | ¦0126¦0592¦1104¦1127¦1609¦9005¦9105¦                                         | clamir_289   | NULL       | 2012-04-03 00:00:00 | 2012-04-03 00:00:00 | clamir_pPM  | clamir_SALLANCH | clamir_SALLANCH | Z           | 2025-01-26 20:24:29.623220+01:00 | 2025-01-26 20:33:24.062586+01:00 |    20.25 |    13.5  |        0 |        0 |        0 | Urbain de densité intermédiaire | AURA          | ARVE           | SAL       | SALLANCHES    | Unknown               | Unknown             | NULL         | NULL     | NULL        |    45.9452 |     6.71907 |             74208 | NULL     | NULL     | NULL     | NULL     | NaT                      |                  nan |                       nan | NULL   | NaT                   | NULL          | NaT                     |                            nan | 2025-05-12 18:24:16.530000+02:00 |
| 2012 |      4 |    22 |   08 | ****** | clamir_pat35572  | clamir_2000008388 | clamir_384313 | clamir_384313    | clamir_VAKSSE | F      |     42.66 |      5.22 |      37.44 | clamir   | ¦0532¦1104¦1141¦1432¦2004¦2007¦9005¦9105¦                                    | clamir_292   | NULL       | 2012-04-03 00:00:00 | 2012-04-03 00:00:00 | clamir_pDJL | clamir_SALLANCH | clamir_SALLANCH | Z           | 2025-01-26 20:24:29.623220+01:00 | 2025-01-26 20:33:24.062586+01:00 |    42.66 |     0    |        0 |        0 |        0 | Urbain de densité intermédiaire | AURA          | ARVE           | SAL       | SALLANCHES    | Unknown               | Unknown             | NULL         | NULL     | NULL        |    45.8966 |     6.81744 |             74143 | NULL     | NULL     | NULL     | NULL     | NaT                      |                  nan |                       nan | NULL   | NaT                   | NULL          | NaT                     |                            nan | 2025-05-12 18:24:16.530000+02:00 |
| 2012 |      4 |    26 |   08 | ****** | clamir_pat10065  | clamir_2000008385 | clamir_384293 | clamir_384293    | clamir_GHILRI | F      |      0    |      0    |       0    | clamir   |                                                                              | clamir_292   | NULL       | 2012-04-03 00:00:00 | 2012-04-03 00:00:00 | clamir_pDSR | clamir_SALLANCH | clamir_SALLANCH | Z           | 2025-01-26 20:24:29.623220+01:00 | 2025-01-26 20:33:24.062586+01:00 |     0    |     0    |        0 |        0 |        0 | Urbain de densité intermédiaire | AURA          | ARVE           | SAL       | SALLANCHES    | Unknown               | Unknown             | NULL         | NULL     | NULL        |    46.0175 |     6.61358 |             74159 | NULL     | NULL     | NULL     | NULL     | NaT                      |                  nan |                       nan | NULL   | NaT                   | NULL          | NaT                     |                            nan | 2025-05-12 18:24:16.530000+02:00 |
| 2012 |      4 |    12 |   08 | ****** | clamir_pat693641 | clamir_2000008384 | clamir_384286 | clamir_384286    | clamir_PELLA  | M      |     22.41 |      0.83 |      21.58 | clamir   | ¦5201¦9005¦9106¦                                                             | clamir_289   | NULL       | 2012-04-03 00:00:00 | 2012-04-03 00:00:00 | clamir_p    | clamir_SALLANCH | clamir_SALLANCH | EXT         | 2025-01-26 20:24:29.623220+01:00 | 2025-01-26 20:33:24.062586+01:00 |    13.45 |     8.96 |        0 |        0 |        0 | Urbain de densité intermédiaire | AURA          | ARVE           | SAL       | SALLANCHES    | Unknown               | Unknown             | NULL         | NULL     | NULL        |    45.9287 |     6.63947 |             74256 | NULL     | NULL     | NULL     | NULL     | NaT                      |                  nan |                       nan | NULL   | NaT                   | NULL          | NaT                     |                            nan | 2025-05-12 18:24:16.530000+02:00 |
| 2012 |      4 |    26 |   08 | ****** | clamir_pat10065  | clamir_2000008386 | clamir_384304 | clamir_384304    | clamir_BOURJA | F      |     56.7  |      2.1  |      54.6  | clamir   | ¦0514¦0522¦0548¦0552¦0563¦0578¦0584¦0592¦1104¦1124¦1208¦1213¦1609¦1804¦9105¦ | clamir_292   | NULL       | 2012-04-03 00:00:00 | 2012-04-03 00:00:00 | clamir_p    | clamir_SALLANCH | clamir_SALLANCH | EXT         | 2025-01-26 20:24:29.623220+01:00 | 2025-01-26 20:33:24.062586+01:00 |    34.02 |    22.68 |        0 |        0 |        0 | Urbain de densité intermédiaire | AURA          | ARVE           | SAL       | SALLANCHES    | Unknown               | Unknown             | NULL         | NULL     | NULL        |    46.0175 |     6.61358 |             74159 | NULL     | NULL     | NULL     | NULL     | NaT                      |                  nan |                       nan | NULL   | NaT                   | NULL          | NaT                     |                            nan | 2025-05-12 18:24:16.530000+02:00 |

### Table: tRHsSIL

**Nombre d'enregistrements:** 128083

| Colonne | Type de données | Nullable | Valeur par défaut |
|---------|----------------|----------|-------------------|
| tRHsSIL | VARCHAR | YES | NULL |
| tRHsSILkey | VARCHAR | YES | NULL |
| Code personne_key | VARCHAR | YES | NULL |
| SAGESAL_key | VARCHAR | YES | NULL |
| SAGESAL_key2 | VARCHAR | YES | NULL |
| NOMPREDDN_tRHsSIL | VARCHAR | YES | NULL |
| NOMPREDDN_tRHsSILkey | VARCHAR | YES | NULL |

#### Exemple de données

*Données masquées - Table identifiée comme sensible*

