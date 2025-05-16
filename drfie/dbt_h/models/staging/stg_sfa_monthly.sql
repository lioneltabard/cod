{{ config(  materialized='incremental',    unique_key='iddemande',    on_schema_change='sync_all_columns',
    pre_hook=[ 'INSTALL parquet;',
            'LOAD parquet;',
            "PRAGMA add_parquet_key('key256', '" ~ env_var('PARQUET_ENCRYPTION_KEY') ~ "');"],    post_hook=[  ]  ) }}

WITH source AS ( 

SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(SEXE AS VARCHAR)as sexe,  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor, 
  CAST(demandedate AS DATE) AS demandedate,  CAST(silsrc as VARCHAR) as silsrc,CAST(silFacture_idDemande as VARCHAR) AS demande_gpskey,CAST(concat(silsrc, '_', numdemande) as VARCHAR) AS numdemande, Age,TH, 
  CAST(NSS13 as VARCHAR) as nss,CAST(silFacture_idDemande as VARCHAR) as silFacture_idDemande ,CAST(ActRTOT AS DECIMAL(10,2)) AS ActRTOT,CAST( MNTPRELfac AS DECIMAL(10,2)) as mntprel, CAST( mntactesfac AS DECIMAL(10,2)) as mntactes ,
  CAST(SDA_CNc_GC AS VARCHAR) as codeNomenclatureGC, split_part(silDemande_saisiePar,'_',2) as silDemande_saisiePar, CAST(CCORi as VARCHAR)  as ccor_ids , CAST(  DFT_NOPAT  as VARCHAR) as idpatient,CAST(codeMedecin as VARCHAR) as medid,
  CAST(validedate AS DATE) AS validedate  ,split_part(CAST(prelevePar as VARCHAR),'_',2)  as prelevePar,CAST(sdem_idSite as VARCHAR) AS sdem_idSite,CAST(Z_DOM_EXT as VARCHAR) as z_dom_ext, qvd_mtime, pqt_mtime 
  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_cla_asfbestgps/*/*/ASFBESTGPS_CLA_MIR*.parquet') 
UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(SEXE AS VARCHAR)as sexe,  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
  CAST(demandedate AS DATE) AS demandedate,  CAST(silsrc as VARCHAR) as silsrc,CAST(silFacture_idDemande as VARCHAR) AS demande_gpskey,CAST(concat(silsrc, '_', numdemande) as VARCHAR) AS numdemande, Age,TH, 
  CAST(NSS13 as VARCHAR) as nss,CAST(silFacture_idDemande as VARCHAR) as silFacture_idDemande ,CAST(ActRTOT AS DECIMAL(10,2)) AS ActRTOT,CAST( MNTPRELfac AS DECIMAL(10,2)) as mntprel, CAST( mntactesfac AS DECIMAL(10,2)) as mntactes ,
  CAST(SDA_CNc_GC AS VARCHAR) as codeNomenclatureGC,split_part(silDemande_saisiePar,'_',2) as  silDemande_saisiePar, CAST(CCORi as VARCHAR)  as ccor_ids , CAST(  DFT_NOPAT  as VARCHAR) as idpatient,CAST(codeMedecin as VARCHAR) as medid,
  CAST(validedate AS DATE) AS validedate  ,split_part(CAST(prelevePar as VARCHAR),'_',2) as prelevePar,CAST(sdem_idSite as VARCHAR) AS sdem_idSite,CAST(Z_DOM_EXT as VARCHAR) as z_dom_ext, qvd_mtime, pqt_mtime 
  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_cla_asfbestgps/*/*/ASFBESTGPS_CLA_LZC*.parquet') 
UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(SEXE AS VARCHAR)as sexe,  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
  CAST(demandedate AS DATE) AS demandedate,  CAST(silsrc as VARCHAR) as silsrc,CAST(silFacture_idDemande as VARCHAR) AS demande_gpskey,CAST(concat(silsrc, '_', numdemande) as VARCHAR) AS numdemande, Age,TH, 
  CAST(NSS13 as VARCHAR) as nss,CAST(silFacture_idDemande as VARCHAR) as silFacture_idDemande ,CAST(ActRTOT AS DECIMAL(10,2)) AS ActRTOT,CAST( MNTPRELfac AS DECIMAL(10,2)) as mntprel, CAST( mntactesfac AS DECIMAL(10,2)) as mntactes ,
  CAST(SDA_CNc_GC AS VARCHAR) as codeNomenclatureGC,split_part(silDemande_saisiePar,'_',2) as  silDemande_saisiePar, CAST(CCORi as VARCHAR)  as ccor_ids , CAST(  DFT_NOPAT  as VARCHAR) as idpatient,CAST(codeMedecin as VARCHAR) as medid,
  CAST(validedate AS DATE) AS validedate  ,split_part(CAST(prelevePar as VARCHAR),'_',2) as prelevePar,CAST(sdem_idSite as VARCHAR) AS sdem_idSite,CAST(Z_DOM_EXT as VARCHAR) as z_dom_ext, qvd_mtime, pqt_mtime 
  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_cla_asfbestgps/*/*/ASFBESTGPS_CLA_LZM*.parquet') 



  UNION ALL 
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, 
      CAST(montantCorrespondantSsReduction AS DECIMAL(10,2)) -  CAST(dcot_mtt_remise_corr AS DECIMAL(10,2))  as mntcor, CAST(pat_sexe AS VARCHAR)as sexe,  CAST(dcot_mtt_remise_corr AS DECIMAL(10,2))     as remcor,
 CAST(demandedate AS DATE) AS demandedate,   lower(SILSRC) as silsrc,             CAST(silFacture_idDemande AS VARCHAR) AS demande_gpskey,         numdemande ,   Age,  TH,cast( dos_noss as VARCHAR) as nss,
    silFacture_idDemande , ActRTOT   , MNTPRELfac as mntprel    , MNTACTESfac as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC  ,saisiePar as silDemande_saisiePar ,CAST(codeCorrespondant as VARCHAR) as ccor_ids ,
    CAST(concat( lower(SILSRC),'_pat',pat_id) as VARCHAR) as idpatient, CAST(med_code as VARCHAR) as medid, CAST(demandedate AS DATE) AS validedate,codepreleveur as prelevePar,sdem_idSite, Z as z_dom_ext ,qvd_mtime,pqt_mtime
  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_odaifconly/*/*/ActsilFacture_IFC_UNI*.parquet') 
  UNION ALL 
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat,
      CAST(montantCorrespondantSsReduction AS DECIMAL(10,2)) -  CAST(dcot_mtt_remise_corr AS DECIMAL(10,2))  as mntcor, CAST(pat_sexe AS VARCHAR)as sexe,   CAST(dcot_mtt_remise_corr AS DECIMAL(10,2))     as remcor,
 CAST(demandedate AS DATE) AS demandedate,   lower(SILSRC) as silsrc,             CAST(silFacture_idDemande AS VARCHAR) AS demande_gpskey,         numdemande ,   Age,  TH,cast( dos_noss as VARCHAR) as nss,
    silFacture_idDemande , ActRTOT   , MNTPRELfac as mntprel    , MNTACTESfac as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC  ,saisiePar as silDemande_saisiePar ,CAST(codeCorrespondant as VARCHAR) as ccor_ids ,
    CAST(concat( lower(SILSRC),'_pat',pat_id) as VARCHAR) as idpatient, CAST(med_code as VARCHAR) as medid, CAST(demandedate AS DATE) AS validedate,codepreleveur as prelevePar,sdem_idSite, Z as z_dom_ext ,qvd_mtime,pqt_mtime
  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_odaifconly/*/*/ActsilFacture_IFC_APH*.parquet') 
  UNION ALL 
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, 
      CAST(montantCorrespondantSsReduction AS DECIMAL(10,2)) -  CAST(dcot_mtt_remise_corr AS DECIMAL(10,2))  as mntcor,CAST(pat_sexe AS VARCHAR)as sexe,   CAST(dcot_mtt_remise_corr AS DECIMAL(10,2))     as remcor,
 CAST(demandedate AS DATE) AS demandedate,   lower(SILSRC) as silsrc,             CAST(silFacture_idDemande AS VARCHAR) AS demande_gpskey,         numdemande ,   Age,  TH,cast( dos_noss as VARCHAR) as nss,
    silFacture_idDemande , ActRTOT   , MNTPRELfac as mntprel    , MNTACTESfac as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC  ,saisiePar as silDemande_saisiePar ,CAST(codeCorrespondant as VARCHAR) as ccor_ids ,
    CAST(concat( lower(SILSRC),'_pat',pat_id) as VARCHAR) as idpatient, CAST(med_code as VARCHAR) as medid, CAST(demandedate AS DATE) AS validedate,codepreleveur as prelevePar,sdem_idSite, Z as z_dom_ext ,qvd_mtime,pqt_mtime
  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_odaifconly/*/*/ActsilFacture_IFC_BMR*.parquet') 
  UNION ALL 
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, 
      CAST(montantCorrespondantSsReduction AS DECIMAL(10,2)) -  CAST(dcot_mtt_remise_corr AS DECIMAL(10,2))  as mntcor, CAST(pat_sexe AS VARCHAR)as sexe,  CAST(dcot_mtt_remise_corr AS DECIMAL(10,2))     as remcor,
 CAST(demandedate AS DATE) AS demandedate,   lower(SILSRC) as silsrc,             CAST(silFacture_idDemande AS VARCHAR) AS demande_gpskey,         numdemande ,   Age,  TH,cast( dos_noss as VARCHAR) as nss,
    silFacture_idDemande , ActRTOT   , MNTPRELfac as mntprel    , MNTACTESfac as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC  ,saisiePar as silDemande_saisiePar ,CAST(codeCorrespondant as VARCHAR) as ccor_ids ,
    CAST(concat( lower(SILSRC),'_pat',pat_id) as VARCHAR) as idpatient, CAST(med_code as VARCHAR) as medid, CAST(demandedate AS DATE) AS validedate,codepreleveur as prelevePar,sdem_idSite, Z as z_dom_ext ,qvd_mtime,pqt_mtime
  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_odaifconly/*/*/ActsilFacture_IFC_MIR*.parquet') 


UNION ALL
--SELECT CAST(DFT_VHDF_MONTANTFACTURE_AMO AS DECIMAL(10,2))/100 as mntcai, CAST(DFT_VHDF_MONTANTFACTURE_AMC AS DECIMAL(10,2))/100 as mntmut, CAST(DFT_VHDF_MONTANTFACTURE_P AS DECIMAL(10,2))/100 as mntpat, 
 --CAST(MNTCOR AS DECIMAL(10,2)) as mntcor,   CAST('0' AS DECIMAL(10,2))      as remcor,

 SELECT      COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMO, '') AS DECIMAL(10,2)), 0)/100 as mntcai,    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMC, '') AS DECIMAL(10,2)), 0)/100 as mntmut,
    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_P, '') AS DECIMAL(10,2)), 0)/100 as mntpat, 
                COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_C  , '') AS DECIMAL(10,2)), 0)/100 as mntcor, CAST(SEXE AS VARCHAR)as sexe, 
    CAST('0' AS DECIMAL(10,2)) as remcor,
 CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc, CAST( numdemande AS VARCHAR) AS demande_gpskey,           numdemande,CASE 
   WHEN DFT_AGEJULIEN IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) > 0 
   THEN CAST(TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) / 365.25 AS INT)            ELSE 50       END as Age, TH,  CAST(concat (NSS13,CLENSS13) AS VARCHAR)as nss, 
   silFacture_idDemande , ActRTOT   , MNTPREL as mntprel    , MNTACTES as mntactes   , codeNomenclatureGC  , DFT_COPER as silDemande_saisiePar  , CAST(CCORi as VARCHAR) as ccor_ids, CAST( concat(lower(SILSRC),'_pat', DFT_NOPAT )as VARCHAR)  as idpatient,
   CAST(DFT_PRESCRIPTEUR as VARCHAR) as medid, validedate,DFT_CPREL as prelevePar,sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_hxl/*/*/ActsilFacture_HXL_ORN*.parquet') 

UNION ALL
 SELECT      COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMO, '') AS DECIMAL(10,2)), 0)/100 as mntcai,    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMC, '') AS DECIMAL(10,2)), 0)/100 as mntmut,
    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_P, '') AS DECIMAL(10,2)), 0)/100 as mntpat,
                COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_C  , '') AS DECIMAL(10,2)), 0)/100 as mntcor, CAST(SEXE AS VARCHAR)as sexe,  
    CAST('0' AS DECIMAL(10,2)) as remcor,
 CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc, CAST( numdemande AS VARCHAR) AS demande_gpskey,           numdemande,CASE 
   WHEN DFT_AGEJULIEN IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) > 0 
   THEN CAST(TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) / 365.25 AS INT)            ELSE 50       END as Age, TH,  CAST(concat (NSS13,CLENSS13) AS VARCHAR)as nss, 
   silFacture_idDemande , ActRTOT   , MNTPREL as mntprel    , MNTACTES as mntactes   , codeNomenclatureGC  , DFT_COPER as silDemande_saisiePar  , CAST(CCORi as VARCHAR)  as ccor_ids  , CAST( concat(lower(SILSRC),'_pat', DFT_NOPAT )as VARCHAR)  as idpatient,
   CAST(DFT_PRESCRIPTEUR as VARCHAR) as medid, validedate,DFT_CPREL as prelevePar,sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_hxl/*/*/ActsilFacture_HXL_MYS*.parquet') 
UNION ALL
 SELECT      COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMO, '') AS DECIMAL(10,2)), 0)/100 as mntcai,    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMC, '') AS DECIMAL(10,2)), 0)/100 as mntmut,
    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_P, '') AS DECIMAL(10,2)), 0)/100 as mntpat,
           COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_C  , '') AS DECIMAL(10,2)), 0)/100 as mntcor,  CAST(SEXE AS VARCHAR)as sexe,        
    CAST('0' AS DECIMAL(10,2)) as remcor,
 CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc, CAST( numdemande AS VARCHAR) AS demande_gpskey,           numdemande,CASE 
   WHEN DFT_AGEJULIEN IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) > 0 
   THEN CAST(TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) / 365.25 AS INT)            ELSE 50       END as Age, TH,  CAST(concat (NSS13,CLENSS13) AS VARCHAR)as nss, 
   silFacture_idDemande , ActRTOT   , MNTPREL as mntprel    , MNTACTES as mntactes   , codeNomenclatureGC  , DFT_COPER as silDemande_saisiePar  , CAST(CCORi as VARCHAR)  as ccor_ids  , CAST( concat(lower(SILSRC),'_pat', DFT_NOPAT )as VARCHAR)  as idpatient,
   CAST(DFT_PRESCRIPTEUR as VARCHAR) as medid, validedate,DFT_CPREL as prelevePar,sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_hxl/*/*/ActsilFacture_HXL_C78*.parquet') 
UNION ALL
 SELECT      COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMO, '') AS DECIMAL(10,2)), 0)/100 as mntcai,    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMC, '') AS DECIMAL(10,2)), 0)/100 as mntmut,
    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_P, '') AS DECIMAL(10,2)), 0)/100 as mntpat,
              COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_C  , '') AS DECIMAL(10,2)), 0)/100 as mntcor, CAST(SEXE AS VARCHAR)as sexe,     
    CAST('0' AS DECIMAL(10,2)) as remcor,
 CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc, CAST( numdemande AS VARCHAR) AS demande_gpskey,           numdemande,CASE 
   WHEN DFT_AGEJULIEN IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) > 0 
   THEN CAST(TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) / 365.25 AS INT)            ELSE 50       END as Age, TH,  CAST(concat (NSS13,CLENSS13) AS VARCHAR)as nss, 
   silFacture_idDemande , ActRTOT   , MNTPREL as mntprel    , MNTACTES as mntactes   , codeNomenclatureGC  , DFT_COPER as silDemande_saisiePar  , CAST(CCORi as VARCHAR)  as ccor_ids  , CAST( concat(lower(SILSRC),'_pat', DFT_NOPAT )as VARCHAR)  as idpatient,
   CAST(DFT_PRESCRIPTEUR as VARCHAR) as medid, validedate,DFT_CPREL as prelevePar,sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_hxl/*/*/ActsilFacture_HXL_AST*.parquet') 

UNION ALL
 SELECT      COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMO, '') AS DECIMAL(10,2)), 0)/100 as mntcai,    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMC, '') AS DECIMAL(10,2)), 0)/100 as mntmut,
    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_P, '') AS DECIMAL(10,2)), 0)/100 as mntpat,
      COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_C  , '') AS DECIMAL(10,2)), 0)/100 as mntcor,   CAST(SEXE AS VARCHAR)as sexe,             
    CAST('0' AS DECIMAL(10,2)) as remcor,
 CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc, CAST( numdemande AS VARCHAR) AS demande_gpskey,           numdemande,CASE 
   WHEN DFT_AGEJULIEN IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) > 0 
   THEN CAST(TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) / 365.25 AS INT)            ELSE 50       END as Age, TH,  CAST(concat (NSS13,CLENSS13) AS VARCHAR)as nss, 
   silFacture_idDemande , ActRTOT   , MNTPREL as mntprel    , MNTACTES as mntactes   , codeNomenclatureGC  , DFT_COPER as silDemande_saisiePar  , CAST(CCORi as VARCHAR)  as ccor_ids  , CAST( concat(lower(SILSRC),'_pat', DFT_NOPAT )as VARCHAR)  as idpatient,
   CAST(DFT_PRESCRIPTEUR as VARCHAR) as medid, validedate,DFT_CPREL as prelevePar,sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_hxl/*/*/ActsilFacture_HXL_LCD*.parquet') 
UNION ALL
 SELECT      COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMO, '') AS DECIMAL(10,2)), 0)/100 as mntcai,    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMC, '') AS DECIMAL(10,2)), 0)/100 as mntmut,
    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_P, '') AS DECIMAL(10,2)), 0)/100 as mntpat,
               COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_C  , '') AS DECIMAL(10,2)), 0)/100 as mntcor, CAST(SEXE AS VARCHAR)as sexe,    
    CAST('0' AS DECIMAL(10,2)) as remcor,
 CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc, CAST( numdemande AS VARCHAR) AS demande_gpskey,           numdemande,CASE 
   WHEN DFT_AGEJULIEN IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) > 0 
   THEN CAST(TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) / 365.25 AS INT)            ELSE 50       END as Age, TH,  CAST(concat (NSS13,CLENSS13) AS VARCHAR)as nss, 
   silFacture_idDemande , ActRTOT   , MNTPREL as mntprel    , MNTACTES as mntactes   , codeNomenclatureGC  , DFT_COPER as silDemande_saisiePar  , CAST(CCORi as VARCHAR)  as ccor_ids  , CAST( concat(lower(SILSRC),'_pat', DFT_NOPAT )as VARCHAR)  as idpatient,
   CAST(DFT_PRESCRIPTEUR as VARCHAR) as medid, validedate,DFT_CPREL as prelevePar,sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_hxl/*/*/ActsilFacture_HXL_BPO*.parquet') 
UNION ALL
 SELECT      COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMO, '') AS DECIMAL(10,2)), 0)/100 as mntcai,    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_AMC, '') AS DECIMAL(10,2)), 0)/100 as mntmut,
    COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_P, '') AS DECIMAL(10,2)), 0)/100 as mntpat,
              COALESCE(TRY_CAST(NULLIF(DFT_VHDF_MONTANTFACTURE_C,   '') AS DECIMAL(10,2)), 0)/100 as mntcor, CAST(SEXE AS VARCHAR)as sexe,    
    CAST('0' AS DECIMAL(10,2)) as remcor,
 CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc, CAST( numdemande AS VARCHAR) AS demande_gpskey,           numdemande,CASE 
   WHEN DFT_AGEJULIEN IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) IS NOT NULL AND TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) > 0 
   THEN CAST(TRY_CAST(DFT_AGEJULIEN AS DECIMAL(10,2)) / 365.25 AS INT)            ELSE 50       END as Age, TH,  CAST(concat (NSS13,CLENSS13) AS VARCHAR)as nss, 
   silFacture_idDemande , ActRTOT   , MNTPREL as mntprel    , MNTACTES as mntactes   , codeNomenclatureGC  , DFT_COPER as silDemande_saisiePar  , CAST(CCORi as VARCHAR)  as ccor_ids  , CAST( concat(lower(SILSRC),'_pat', DFT_NOPAT )as VARCHAR)  as idpatient,
   CAST(DFT_PRESCRIPTEUR as VARCHAR) as medid, validedate,DFT_CPREL as prelevePar,sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_hxl/*/*/ActsilFacture_HXL_BSY*.parquet') 


   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
  CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc, CAST(  numdemande AS VARCHAR)  AS demande_gpskey,  numdemande, Age,TH,CAST( sdem_assureNumSecu as VARCHAR)as nss,
   silFacture_idDemande , ActRTOT   , MNTPREL as mntprel    , MNTACTES as mntactes  , codeNomenclatureGC  , silDemande_saisiePar  , CCORi  as ccor_ids , concat(lower(SILSRC),'_pat',idPatient) as idpatient, CAST(MEDPLid as VARCHAR) as medid,
   validedate  ,prelevePar   , sdem_idSite,   Z_DOM_EXT  as z_dom_ext ,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl_old/*/*/ActsilFacture_BLN_*.parquet') 
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
  CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc, CAST(  numdemande AS VARCHAR)  AS demande_gpskey,  numdemande, Age,TH,CAST( sdem_assureNumSecu as VARCHAR)as nss,
   silFacture_idDemande , ActRTOT   , MNTPREL as mntprel    , MNTACTES as mntactes  , codeNomenclatureGC  , silDemande_saisiePar  , CCORi  as ccor_ids , concat(lower(SILSRC),'_pat',idPatient) as idpatient,CAST(MEDPLid as VARCHAR) as medid,
   validedate  ,prelevePar   , sdem_idSite,   Z_DOM_EXT  as z_dom_ext ,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl_old/*/*/ActsilFacture_BMD_*.parquet') 

   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,  
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_A2B*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_AST*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_B86*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_BCL*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_BMG*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_BMR*.parquet')   

   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_BPO*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_BSY*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_CAB*.parquet')   

   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_CBM*.parquet')   

   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_DYO*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
  CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_EMR*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_GLB*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_GNV*.parquet')   

   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_LCD*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_LMA*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_LZB*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
  CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_LZC*.parquet')   


   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
  CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_MIR*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_OME*.parquet')   

   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
  CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_ORN*.parquet')   

   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_RYL*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_SLB*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_SMB*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_TRL*.parquet')   
  
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_RSB*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_BLT*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
  CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_BST*.parquet')   
   UNION ALL
SELECT CAST(montantCaisse AS DECIMAL(10,2)) as mntcai, CAST(montantMutuelle AS DECIMAL(10,2)) as mntmut, CAST(montantPatient AS DECIMAL(10,2)) as mntpat, CAST(montantCorrespondant AS DECIMAL(10,2)) as mntcor,
  CAST(silPatient_sexe AS VARCHAR)as sexe,
  CAST(montantCorrespondantSsReduction AS DECIMAL(10,2))  -  CAST(montantCorrespondant AS DECIMAL(10,2))    as remcor,
   CAST(demandedate AS DATE) AS demandedate, lower(SILSRC) as silsrc,CAST(concat(silsrc, '_', numdemande) AS VARCHAR) as demande_gpskey, concat(silsrc, '_', numdemande) AS numdemande,Age, TH, CAST( sdem_assureNumSecu as VARCHAR)as nss, 
 concat(lower(SILSRC),'_',silFacture_idDemande) AS silFacture_idDemande,ActRTOT,MNTPREL as mntprel,MNTACTES as mntactes,CAST(codeNomenclatureGC as VARCHAR) as codeNomenclatureGC,silDemande_saisiePar,cast(CCORi as VARCHAR) as ccor_ids,concat(lower(SILSRC),'_pat',sdem_idPatient) as idpatient,
 CAST(MEDPLid as VARCHAR) as medid,validedate ,preleveParF as prelevePar, sdem_idSite,Z_DOM_EXT as z_dom_ext,qvd_mtime,pqt_mtime  FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_asfl/*/*/ActsilFacture_L_LRB*.parquet')   
              ),




renamed AS (  SELECT  DATE_PART('year', demandedate) AS an, DATE_PART('month', demandedate) AS mois, CAST( Age as INT)  as Age,TH,nss, CAST(idpatient as VARCHAR) as idpatient ,
                      numdemande, silFacture_idDemande AS iddemande,   demande_gpskey, concat(silsrc, '_',  medid  ) AS medid, CAST(sexe as VARCHAR) as sexe ,
          CAST(actrtot AS DECIMAL(10,2)) AS actrtot,   mntprel,mntactes,          silsrc,
          CAST(codeNomenclatureGC as VARCHAR) as cnomencgc,        concat(silsrc, '_',  silDemande_saisiePar  ) AS saisie_par, CAST(ccor_ids as VARCHAR) as  ccor_ids,  demandedate,  CAST(validedate AS DATE) AS validedate,
        concat( silsrc, '_p', prelevePar) AS prel_par, {{ idsite_prefix('sdem_idSite', 'silsrc', 'demandedate') }} AS idsite,  sdem_idsite,z_dom_ext,qvd_mtime,pqt_mtime,  mntcai,mntmut,mntpat,mntcor,remcor
    FROM source
),

with_sector AS (
    SELECT 
        renamed.*,
        COALESCE(sector_mapping.TYPE_BASSIN_VIE, 'Unknown') AS tbv_dbtm, 
        COALESCE(sector_mapping.REGION, 'Unknown') AS region_dbtm, 
        COALESCE(sector_mapping.SECTEUR, 'Unknown') AS secteur_dbtm, 
        COALESCE(sector_mapping.xx, 'Unknown') AS xx_dbtm, 
        COALESCE(sector_mapping.nomKSL, 'Unknown') AS nomksl_dbtm,
        COALESCE(personnel_mapping.nss, 'Unknown') AS saisie_par_nss_dbtm,
        COALESCE(preleveur_mapping.nss, 'Unknown') AS prel_par_nss_dbtm,
        pharm.sil_idcorrespondant AS pharmacieg,
        ehp.sil_idcorrespondant AS ehpadg,
        clin.sil_idcorrespondant AS cliniqueg
    FROM renamed
    LEFT JOIN {{ ref('mapping_idsite_labos') }} AS sector_mapping 
        ON renamed.idsite = sector_mapping.idsite
    LEFT JOIN {{ ref('mapping_SILs_idPersonnels_rhs') }} AS personnel_mapping 
        ON renamed.saisie_par = personnel_mapping.SILs_idPersonnels
    LEFT JOIN {{ ref('mapping_pcod_rhs') }} AS preleveur_mapping 
        ON renamed.prel_par = preleveur_mapping.pcod
    LEFT JOIN (
        SELECT DISTINCT ccor_ids, 
               FIRST_VALUE(sil_idcorrespondant) OVER (PARTITION BY ccor_ids ORDER BY sil_idcorrespondant) AS sil_idcorrespondant
        FROM {{ ref('mapping_SILs_idCorrespondants_cps') }}
        CROSS JOIN (SELECT DISTINCT ccor_ids FROM renamed) AS r
        WHERE r.ccor_ids LIKE '%|' || sil_idcorrespondant || '|%'
        AND type_cps = 'Pharmacie'
    ) AS pharm ON renamed.ccor_ids = pharm.ccor_ids
    LEFT JOIN (
        SELECT DISTINCT ccor_ids, 
               FIRST_VALUE(sil_idcorrespondant) OVER (PARTITION BY ccor_ids ORDER BY sil_idcorrespondant) AS sil_idcorrespondant
        FROM {{ ref('mapping_SILs_idCorrespondants_cps') }}
        CROSS JOIN (SELECT DISTINCT ccor_ids FROM renamed) AS r
        WHERE r.ccor_ids LIKE '%|' || sil_idcorrespondant || '|%'
        AND type_cps = 'EHPAD'
    ) AS ehp ON renamed.ccor_ids = ehp.ccor_ids
    LEFT JOIN (
        SELECT DISTINCT ccor_ids, 
               FIRST_VALUE(sil_idcorrespondant) OVER (PARTITION BY ccor_ids ORDER BY sil_idcorrespondant) AS sil_idcorrespondant
        FROM {{ ref('mapping_SILs_idCorrespondants_cps') }}
        CROSS JOIN (SELECT DISTINCT ccor_ids FROM renamed) AS r
        WHERE r.ccor_ids LIKE '%|' || sil_idcorrespondant || '|%'
        AND type_cps IN ('clinique', 'ssr')
    ) AS clin ON renamed.ccor_ids = clin.ccor_ids
),

gps as (  
           SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_A2B*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_AST*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_B86*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_BCL*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_BMG*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_BMR*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_BPO*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_BSY*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_CAB*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_CBM*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_DYO*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_EMR*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_GLB*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_GNV*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_LCD*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_LMA*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_LZB*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_LZC*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_MIR*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_OME*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_ORN*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_RYL*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_SLB*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_SMB*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_TRL*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_LRB*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_BLT*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_BST*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gps/*/*/silDemandeGps_RSB*.parquet') 

UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gpshxl/silDemandeGps_ORN*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gpshxl/silDemandeGps_MYS*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gpshxl/silDemandeGps_C78*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gpshxl/silDemandeGps_BPO*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gpshxl/silDemandeGps_BSY*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gpshxl/silDemandeGps_LCD*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey,CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,
CAST(result_citycode as VARCHAR) as result_citycode FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gpshxl/silDemandeGps_AST*.parquet') 

UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(silFacture_idDemande as VARCHAR) as demande_gpskey, CAST(mapLAT as VARCHAR) as latitude, CAST(mapLNG as VARCHAR) as longitude,CAST(mapCITYCODE as VARCHAR) as result_citycode  
FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_cla_asfbestgps/*/*/ASFBESTGPS_CLA_MIR*.parquet')
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(silFacture_idDemande as VARCHAR) as demande_gpskey, CAST(mapLAT as VARCHAR) as latitude, CAST(mapLNG as VARCHAR) as longitude,CAST(mapCITYCODE as VARCHAR) as result_citycode  
FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_cla_asfbestgps/*/*/ASFBESTGPS_CLA_LZC*.parquet')
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(silFacture_idDemande as VARCHAR) as demande_gpskey, CAST(mapLAT as VARCHAR) as latitude, CAST(mapLNG as VARCHAR) as longitude,CAST(mapCITYCODE as VARCHAR) as result_citycode  
FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_cla_asfbestgps/*/*/ASFBESTGPS_CLA_LZM*.parquet')


UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey, CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,CAST(result_citycode AS VARCHAR) as result_citycode 
FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gpsifc/silDemandeGps_UNI*_NEW.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey, CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,CAST(result_citycode AS VARCHAR) as result_citycode 
FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gpsifc/silDemandeGps_APH*_NEW.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey, CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,CAST(citycode AS VARCHAR) as result_citycode 
FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gpsifc/silDemandeGps_BMR*.parquet') 
UNION ALL SELECT CAST(numdemande as VARCHAR) as numdemande, CAST(demande_gpskey as VARCHAR) as demande_gpskey, CAST(latitude as VARCHAR) as  latitude,CAST(longitude as VARCHAR) as  longitude,CAST(result_citycode AS VARCHAR) as result_citycode 
FROM read_parquet('{{ var("base_path") }}/fromairflow/pqt4dbt_gpsifc/silDemandeGps_MIR*_NEW.parquet') 
),

cm as (
  SELECT CAST(idDemande as VARCHAR) as iddemandekey, CAST(CAIrgm as VARCHAR) as cairgm, CAST(CAIcod as VARCHAR) as caicod, CAST(MUTcod as VARCHAR) as  mutcod, CAST(CAIdpt as VARCHAR) as caidpt
  FROM read_csv('{{ var("base_path") }}/temp/silFactureCM_*.csv', delim=';', all_varchar=true ) 
),

sfa AS (    
SELECT idDemande, sdem_ordonnanceDateISO,delai_demande_ordo,delai_saisie_validation FROM read_parquet('G:/root/activites/ksl/A2B/MENS_ACMC/SFA_*.parquet',encryption_config={'footer_key': 'key256'}    )
UNION ALL 
SELECT idDemande, sdem_ordonnanceDateISO,delai_demande_ordo,delai_saisie_validation FROM read_parquet('G:/root/activites/ksl/AST/MENS_ACMC/SFA_*.parquet',encryption_config={'footer_key': 'key256'}    )
UNION ALL 
SELECT idDemande, sdem_ordonnanceDateISO,delai_demande_ordo,delai_saisie_validation FROM read_parquet('G:/root/activites/ksl/B86/MENS_ACMC/SFA_*.parquet',encryption_config={'footer_key': 'key256'}    )
UNION ALL 

SELECT idDemande, sdem_ordonnanceDateISO,delai_demande_ordo,delai_saisie_validation FROM read_parquet('G:/root/activites/ksl/BCL/MENS_ACMC/SFA_*.parquet',encryption_config={'footer_key': 'key256'}    )
UNION ALL 
SELECT idDemande, sdem_ordonnanceDateISO,delai_demande_ordo,delai_saisie_validation FROM read_parquet('G:/root/activites/ksl/BMG/MENS_ACMC/SFA_*.parquet',encryption_config={'footer_key': 'key256'}    )
UNION ALL 

SELECT idDemande, sdem_ordonnanceDateISO,delai_demande_ordo,delai_saisie_validation FROM read_parquet('G:/root/activites/ksl/DYO/MENS_ACMC/SFA_*.parquet',encryption_config={'footer_key': 'key256'}    )
UNION ALL 
SELECT idDemande, sdem_ordonnanceDateISO,delai_demande_ordo,delai_saisie_validation FROM read_parquet('G:/root/activites/ksl/GLB/MENS_ACMC/SFA_*.parquet',encryption_config={'footer_key': 'key256'}    )
UNION ALL 
SELECT idDemande, sdem_ordonnanceDateISO,delai_demande_ordo,delai_saisie_validation FROM read_parquet('G:/root/activites/ksl/MIR/MENS_ACMC/SFA_*.parquet',encryption_config={'footer_key': 'key256'}    )
UNION ALL 
SELECT idDemande, sdem_ordonnanceDateISO,delai_demande_ordo,delai_saisie_validation FROM read_parquet('G:/root/activites/ksl/ORN/MENS_ACMC/SFA_*.parquet',encryption_config={'footer_key': 'key256'}    )
UNION ALL 
SELECT idDemande,sdem_ordonnanceDateISO,delai_demande_ordo,delai_saisie_validation FROM read_parquet('G:/root/activites/ksl/SMB/MENS_ACMC/SFA_*.parquet',encryption_config={'footer_key':'key256'})
UNION ALL 
SELECT idDemande,sdem_ordonnanceDateISO,delai_demande_ordo,delai_saisie_validation FROM read_parquet('G:/root/activites/ksl/TRL/MENS_ACMC/SFA_*.parquet',encryption_config={'footer_key':'key256'})

    ),


--sdp AS ( 
-- SELECT  concat('kslast_',idDemande)  as   idDemande, PQLG   FROM read_parquet( 'G:/root/activites/ksl/ast/_paranmois/silDemandePrelevement_*_G_summary.parquet',  encryption_config={'footer_key': 'key256'}    )
-- UNION ALL
-- SELECT  concat('ksldyo_',idDemande)  as   idDemande, PQLG   FROM read_parquet( 'G:/root/activites/ksl/dyo/_paranmois/silDemandePrelevement_*_G_summary.parquet',  encryption_config={'footer_key': 'key256'}    )

--),


sdp AS ( 
SELECT concat('kslast_',idDemande) as idDemande,PQLG,receptionDatetimeISOG,preleveDatetimeISOG,delai_prelevement_receptionG,preleveParG FROM read_parquet( 'G:/root/activites/ksl/ast/_paranmois/silDemandePrelevement_*_G_summary.parquet',  encryption_config={'footer_key': 'key256'}    )
UNION ALL
SELECT concat('ksldyo_',idDemande) as idDemande,PQLG,receptionDatetimeISOG,preleveDatetimeISOG,delai_prelevement_receptionG,preleveParG FROM read_parquet( 'G:/root/activites/ksl/dyo/_paranmois/silDemandePrelevement_*_G_summary.parquet',  encryption_config={'footer_key': 'key256'}    )
UNION ALL
SELECT concat('kslsmb_',idDemande) as idDemande,PQLG,receptionDatetimeISOG,preleveDatetimeISOG,delai_prelevement_receptionG,preleveParG FROM read_parquet( 'G:/root/activites/ksl/smb/_paranmois/silDemandePrelevement_*_G_summary.parquet',  encryption_config={'footer_key': 'key256'}    )
UNION ALL
SELECT concat('ksltrl_',idDemande) as idDemande,PQLG,receptionDatetimeISOG,preleveDatetimeISOG,delai_prelevement_receptionG,preleveParG FROM read_parquet( 'G:/root/activites/ksl/trl/_paranmois/silDemandePrelevement_*_G_summary.parquet',  encryption_config={'footer_key': 'key256'}    )

),


-- final_data AS (
--    SELECT 
--        ws.*,    
--        gps.latitude,    
--        gps.longitude,
--        gps.result_citycode,  cm.cairgm, cm.caidpt, cm.caicod,  cm.mutcod, sfa.sdem_ordonnancedateiso ,sfa.delai_demande_ordo,sfa.delai_saisie_validation,
--        sdp.pqlg,
--        CURRENT_TIMESTAMP AS updated_at 
--    FROM with_sector ws
--    LEFT JOIN gps ON ws.demande_gpskey = gps.demande_gpskey  
--    LEFT JOIN cm  ON ws.iddemande      = cm.iddemandekey  
--    LEFT JOIN sfa  ON ws.iddemande      = sfa.iddemande  
--    LEFT JOIN sdp  ON ws.iddemande      = sdp.iddemande  
-- )


final_data AS (
    SELECT 
        ws.*,  
        gps.latitude,    
        gps.longitude,
        gps.result_citycode,  cm.cairgm, cm.caidpt, cm.caicod,  cm.mutcod, sfa.sdem_ordonnancedateiso ,sfa.delai_demande_ordo,sfa.delai_saisie_validation,
        sdp.pqlg, sdp.prelevedatetimeisog, sdp.preleveparg,sdp.receptiondatetimeisog, sdp.delai_prelevement_receptiong,
        CURRENT_TIMESTAMP AS updated_at 
    FROM with_sector ws
   
    LEFT JOIN gps ON ws.demande_gpskey = gps.demande_gpskey  
    LEFT JOIN cm  ON ws.iddemande      = cm.iddemandekey  
    LEFT JOIN sfa  ON ws.iddemande      = sfa.iddemande  
    LEFT JOIN sdp  ON ws.iddemande      = sdp.iddemande  
)



SELECT * 
FROM final_data
{% if is_incremental() %}
WHERE                                                                                                      --    silsrc  in ('ksldyo','ifcuni','hxlorn','kslorn')  AND
    -- Insert if iddemande doesn't exist
    iddemande NOT IN (SELECT iddemande FROM {{ this }})
    OR 
    -- Replace if iddemande exists and pqt_mtime is more recent
    (
        iddemande IN (SELECT iddemande FROM {{ this }})
        AND pqt_mtime > (SELECT pqt_mtime FROM {{ this }} WHERE iddemande = final_data.iddemande)
    )
{% endif %}


