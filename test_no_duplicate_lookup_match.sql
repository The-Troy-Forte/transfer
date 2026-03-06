-- =============================================================================
-- Singular test : test_no_duplicate_lookup_match.sql
-- Origin        : Informatica Mapplet  mplt_src_cd
--                 Lookup policy "Report Error" on multiple matches applied to:
--                   • LKP_CD_DOMAIN_KEY  (lookup table: CIM.CD_DOMAIN)
--                       condition: CD_DOMAIN_NME = LKP_CD_DOMAIN_NME
--                   • LKP_SRC_CD_KEY     (lookup table: CIM.SRC_CD, CURR_INDC=1)
--                       condition: CD_DOMAIN_KEY = LKP_CD_DOMAIN_KEY
--                              AND REC_SRC_CD    = LKP_REC_SRC_CD
--                              AND SRC_CD        = LKP_SRC_CD
--
-- dbt contract  : a passing test returns zero rows.
--                 Any row returned = duplicate match = test FAILS.
--
-- Structure:
--   Part 1 – lkp_cd_domain_dupes   replicates LKP_CD_DOMAIN_KEY duplicate check
--   Part 2 – lkp_src_cd_dupes      replicates LKP_SRC_CD_KEY duplicate check
--   Part 3 – UNION ALL + label      surfaces which lookup failed and why
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- Part 1 : LKP_CD_DOMAIN_KEY
--   Lookup SQL (original):
--     SELECT CD_DOMAIN_KEY, UPPER(CD_DOMAIN_NME) AS CD_DOMAIN_NME
--     FROM CIM.CD_DOMAIN
--   Join condition: CD_DOMAIN_NME = LKP_CD_DOMAIN_NME
--   Normalisation applied in stg_cd_domain: UPPER(cd_domain_nme)
--   A domain name must resolve to exactly one CD_DOMAIN_KEY.
-- ─────────────────────────────────────────────────────────────────────────────
with lkp_cd_domain_dupes as (

    select
        cd_domain_nme                   as lookup_key,
        count(*)                        as match_count,
        'LKP_CD_DOMAIN_KEY'             as lookup_name,
        'CD_DOMAIN_NME'                 as lookup_condition,
        array_agg(
            cast(cd_domain_key as varchar)
            order by cd_domain_key
        )                               as matched_keys

    from {{ ref('stg_cd_domain') }}

    group by cd_domain_nme
    having count(*) > 1

),

-- ─────────────────────────────────────────────────────────────────────────────
-- Part 2 : LKP_SRC_CD_KEY
--   Lookup SQL (original):
--     SELECT SRC_CD_KEY, CD_DOMAIN_KEY, REC_SRC_CD,
--            UPPER(NVL(SRC_CD,'N/A')) AS SRC_CD
--     FROM CIM.SRC_CD WHERE CURR_INDC = 1
--   Join condition: CD_DOMAIN_KEY = LKP_CD_DOMAIN_KEY
--                   AND REC_SRC_CD = LKP_REC_SRC_CD
--                   AND SRC_CD     = LKP_SRC_CD
--   The triple-column composite must resolve to exactly one SRC_CD_KEY.
-- ─────────────────────────────────────────────────────────────────────────────
lkp_src_cd_dupes as (

    select
        -- composite lookup key rendered as a single string for reporting
        cd_domain_key || ' | ' || rec_src_cd || ' | ' || src_cd
                                        as lookup_key,
        count(*)                        as match_count,
        'LKP_SRC_CD_KEY'                as lookup_name,
        'CD_DOMAIN_KEY AND REC_SRC_CD AND SRC_CD'
                                        as lookup_condition,
        array_agg(
            cast(src_cd_key as varchar)
            order by src_cd_key
        )                               as matched_keys

    from {{ ref('stg_src_cd_lkp') }}

    group by cd_domain_key, rec_src_cd, src_cd
    having count(*) > 1

),

-- ─────────────────────────────────────────────────────────────────────────────
-- Part 3 : Union both failure sets into a single result
--   Every row returned causes the dbt test to FAIL and surfaces:
--     lookup_name      – which Infa lookup triggered the error
--     lookup_condition – the join keys that produced the duplicate
--     lookup_key       – the actual key value(s) that collided
--     match_count      – how many rows matched (must be 1; >1 = error)
--     matched_keys     – array of the duplicate surrogate keys found
-- ─────────────────────────────────────────────────────────────────────────────
all_duplicates as (

    select * from lkp_cd_domain_dupes
    union all
    select * from lkp_src_cd_dupes

)

select
    lookup_name,
    lookup_condition,
    lookup_key,
    match_count,
    matched_keys

from all_duplicates

-- Zero rows → test passes (no duplicate lookup matches found)
-- One or more rows → test fails (Informatica "Report Error" condition met)
