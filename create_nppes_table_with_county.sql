CREATE OR REPLACE PROCEDURE create_nppes_table_with_county()
LANGUAGE plpgsql
AS $$
BEGIN

    CREATE OR REPLACE VIEW nppes_summary_with_county AS
        WITH gov_census AS (
            SELECT 
                CONCAT(state, county) AS county,
                "b01001_001e" AS population
            FROM api_census_gov
        ),
        ranked_counties_by_pop AS (
            SELECT 
                zc.zip AS zip_code,
                gc.county,
                gc.population,
                sf.countyname_fips AS county_name,
                ROW_NUMBER() OVER (PARTITION BY zc.zip ORDER BY gc.population DESC) AS highest_pop_county_by_zip_rank
            FROM gov_census gc
            JOIN zip_county_032025 zc ON zc.county = gc.county
            JOIN ssa_fips_state_county_2025 sf ON sf.fipscounty = gc.county
        )
        SELECT 
            nr.npi,
            nr.entity_type,
            nr.entity_name,
            nr.practice_address,
            nr.taxonomy_code,
            nr.grouping,
            nr.classification,
            nr.specialization,
            nr.zip_code,
            rc.county_name AS county_with_highest_pop
        FROM nppes_raw_summary nr
        JOIN ranked_counties_by_pop rc ON nr.zip_code = rc.zip_code
        WHERE rc.highest_pop_county_by_zip_rank = 1;
END;
$$;