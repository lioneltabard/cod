Contexte Business :
Il s'agit d'un réseau d'une trentaine de laboratoires d'analyses médicales, chacun utilisant son propre ERP (système de gestion). Ces laboratoires sont organisés en SECTEURS et MINISECTEURS. Les employés peuvent travailler dans différents laboratoires au sein du même secteur/minisecteur.

Les ERP sont de 4 familles  ( KSL , HXL, CLA, IFC )


# Contexte Biogroup - Réseau de Laboratoires d'Analyses Médicales

## 1. Présentation générale

Biogroup est un réseau comprenant une trentaine de laboratoires d'analyses médicales répartis géographiquement et organisés hiérarchiquement. Chaque laboratoire utilise son propre système de gestion (ERP), regroupés en 4 familles principales :
- **KSL** (Kalisil): Utilisé dans une grande partie des laboratoires
- **HXL** (Hexalab): Système utilisé par plusieurs laboratoires principalement dans la région AURA
- **CLA** (Clarilab): Système utilisé notamment pour les laboratoires Mirialis
- **IFC** (Ifc-Uni): Autre système utilisé dans plusieurs laboratoires

L'organisation hiérarchique est structurée en :
- **SECTEURS** : Zones géographiques larges (Arve, Grenoble Est, etc.)
- **MINISECTEURS** : Subdivisions des secteurs
- **LABORATOIRES** : Points de service individuels

## 2. Structure organisationnelle

### 2.1 Hiérarchie administrative
- Le personnel peut être affecté à plusieurs laboratoires au sein d'un même secteur/minisecteur.
- Les laboratoires sont identifiés par des codes uniques (xx_dbtm) et appartiennent à des entités juridiques spécifiques.
- Des plateaux techniques (PT) servent de centres d'analyses centralisés pour plusieurs laboratoires.

### 2.2 Classification géographique
- **TYPE_BASSIN_VIE** : Classification des laboratoires selon leur environnement (Urbain dense, Urbain de densité intermédiaire, etc.)
- **RÉGION** : Organisation régionale (AURA, GdOUEST, etc.)
- Les codes postaux et codes citycode permettent l'analyse géographique précise.

## 3. Structure des données - Table stg_sfa_monthly

La table centrale `stg_sfa_monthly` contient les données des commandes (demandes d'analyses) avec 57 champs répartis en plusieurs catégories:

### 3.1 Informations temporelles
- **an** (BIGINT) : Année de la commande
- **mois** (BIGINT) : Mois de la commande
- **demandedate** (DATE) : Date de la commande
- **TH** (VARCHAR) : Heure d'enregistrement de la commande dans l'ERP
- **validedate** (DATE) : Date de livraison/validation de la commande
- **sdem_ordonnanceDateISO** (TIMESTAMP) : Date de l'ordonnance médicale
- **delai_demande_ordo** (DOUBLE) : Délai entre l'ordonnance et la demande
- **delai_saisie_validation** (DOUBLE) : Délai entre la saisie et la validation
- **preleveDatetimeISOG** (TIMESTAMP) : Date et heure du prélèvement
- **receptionDatetimeISOG** (TIMESTAMP) : Date et heure de réception
- **delai_prelevement_receptionG** (DOUBLE) : Délai entre prélèvement et réception

### 3.2 Informations patient
- **Age** (INTEGER) : Âge du patient
- **sexe** (VARCHAR) : Genre du patient
- **nss** (VARCHAR) : Numéro de sécurité sociale normalisé
- **idpatient** (VARCHAR) : Identifiant du patient dans l'ERP
- **latitude** / **longitude** (VARCHAR) : Coordonnées géographiques du patient
- **result_citycode** (VARCHAR) : Code postal du patient

### 3.3 Informations commande
- **numdemande** (VARCHAR) : Référence de la commande
- **iddemande** (VARCHAR UNIQUE) : Identifiant unique de la commande
- **demande_gpskey** (VARCHAR) : Clé GPS liée à la commande
- **z_dom_ext** (VARCHAR) : Canal de réception (DIRECT, DOMICILE, INDIRECT)
- **cnomencgc** (VARCHAR) : Liste des actes/analyses normalisés, séparés par des virgules
- **ccor_ids** (VARCHAR) : Liste des partenaires de la commande (établissements médicaux)
- **PQLG** (VARCHAR) : Indicateur de qualité du prélèvement

### 3.4 Informations financières
- **actrtot** (DECIMAL(10,2)) : Montant net total de la commande
- **mntprel** (DOUBLE) : Montant net des prélèvements
- **mntactes** (VARCHAR) : Montant net des actes
- **mntcai** (DOUBLE) : Montant pris en charge par la caisse (Sécurité sociale)
- **mntmut** (DOUBLE) : Montant pris en charge par la mutuelle
- **mntpat** (DOUBLE) : Montant à la charge du patient
- **mntcor** (DOUBLE) : Montant facturé au correspondant
- **remcor** (DECIMAL) : Remise accordée au correspondant
- **cairgm** (VARCHAR) : Régime de la caisse
- **caidpt** (VARCHAR) : Département de la caisse
- **caicod** (VARCHAR) : Code de la caisse
- **mutcod** (VARCHAR) : Code de la mutuelle

### 3.5 Informations organisation
- **silsrc** (VARCHAR) : Instance de l'ERP source
- **idsite** (VARCHAR) : Identifiant normalisé du laboratoire
- **sdem_idSite** (VARCHAR) : Identifiant du site dans l'ERP source
- **tbv_dbtm** (VARCHAR) : Type de bassin de vie
- **region_dbtm** (VARCHAR) : Région du laboratoire
- **secteur_dbtm** (VARCHAR) : Secteur du laboratoire
- **xx_dbtm** (VARCHAR) : Code court du laboratoire
- **nomksl_dbtm** (VARCHAR) : Nom du laboratoire
- **pharmacieg** / **ehpadg** / **cliniqueg** (VARCHAR) : Identifiants des partenaires associés

### 3.6 Informations personnel
- **saisie_par** (VARCHAR) : Identifiant de l'employé enregistrant la commande
- **saisie_par_nss_dbtm** (VARCHAR) : Identifiant normalisé de l'employé enregistrant
- **prel_par** (VARCHAR) : Identifiant du préleveur
- **prel_par_nss_dbtm** (VARCHAR) : Identifiant normalisé du préleveur
- **preleveParG** (VARCHAR) : Identifiant du préleveur (autre format)
- **medid** (VARCHAR) : Identifiant du professionnel prescripteur

### 3.7 Métadonnées
- **qvd_mtime** (TIMESTAMP WITH TIME ZONE) : Date de modification du fichier QVD source
- **pqt_mtime** (TIMESTAMP WITH TIME ZONE) : Date de modification du fichier Parquet source
- **updated_at** (TIMESTAMP WITH TIME ZONE) : Date de dernière mise à jour

## 4. Tables de référence et mappings

Plusieurs tables de référence permettent d'enrichir les données de la table principale :

### 4.1 Informations géographiques
- **LABOs_enrichi_bassins_de_vie** : Contient les informations complètes sur les laboratoires, leurs caractéristiques géographiques et administratives
- **mapping_idsite_labos** : Fait le lien entre les identifiants des sites et leurs informations (secteur, minisecteur, etc.)

### 4.2 Informations personnels
- **mapping_SILs_idPersonnels_rhs** : Fait le lien entre les identifiants des personnels dans les différents ERP et leurs identifiants normalisés
- **mapping_pcod_rhs** : Fait le lien entre les codes des préleveurs et leurs identifiants normalisés
- **tRHsSIL** : Contient les informations RH des employés

### 4.3 Informations partenaires
- **mapping_SILs_idCorrespondants_cps** : Fait le lien entre les identifiants des correspondants (partenaires) et leurs types
- **id_pharmacie_mapping** : Identifie spécifiquement les pharmacies partenaires

### 4.4 Autres tables importantes
- **FAITS_ADDgpn** : Données d'activité du personnel
- **FAITS_MASSALgpn** : Données salariales
- **REFERENTIEL_SITE_ERP** : Référentiel des sites et leurs caractéristiques
- **silpatient** : Données des patients
- **sildemanderesultat** : Résultats des analyses

## 5. Cas d'utilisation analytiques

Cette structure de données permet d'effectuer de nombreuses analyses, notamment :

### 5.1 Analyses de performance
- Suivi de l'activité temporelle (par jour, mois, année)
- Analyse des délais (prélèvement-réception, demande-validation)
- Mesure de la charge de travail par employé et par laboratoire

### 5.2 Analyses financières
- Répartition des montants par type de prise en charge
- Suivi du chiffre d'affaires par secteur, minisecteur, laboratoire
- Analyse des remises accordées aux partenaires

### 5.3 Analyses marketing
- Distribution géographique des patients
- Segmentation par âge et genre
- Analyse des canaux de réception (direct, domicile, externe)

### 5.4 Analyses opérationnelles
- Suivi des types d'analyses les plus demandées
- Performance des préleveurs
- Relation avec les médecins prescripteurs et partenaires (pharmacies, EHPAD, cliniques)

## 6. Architecture technique

Le système de données est structuré autour de:
- Fichiers sources au format Parquet (pqt4dbt)
- Une base de données intermédiaire DuckDB pour les transformations
- Un modèle de données en étoile avec dimension temporelle, géographique, patients, personnels et mesures
- Des processus ETL via DBT (visible dans les configurations du fichier stg_sfa_monthly.sql)
