CREATE INDEX IF NOT EXISTS idx_nppes_raw_npi ON nppes_raw("NPI");
CREATE INDEX IF NOT EXISTS idx_nucc_taxonomy_250_code ON nucc_taxonomy_250("Code");
CREATE INDEX IF NOT EXISTS idx_zip ON zip_county_032025("ZIP");
CREATE INDEX IF NOT EXISTS idx_nppes_zip ON nppes_raw("Provider Business Practice Location Address Postal Code");
CREATE INDEX IF NOT EXISTS idx_county_zip ON zip_county_032025("COUNTY");
CREATE INDEX IF NOT EXISTS idx_county_ssa ON ssa_fips_state_county_2025(fipscounty);

CREATE OR REPLACE PROCEDURE create_nppes_csv(src_table_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    view_name TEXT := src_table_name || '_summary';
BEGIN
    EXECUTE format($f$
		DROP VIEW IF EXISTS %I;

        CREATE OR REPLACE VIEW %I AS
        WITH entity_names AS (
            SELECT 
                "NPI",
                CASE 
                    WHEN "Provider Organization Name (Legal Business Name)" IS NOT NULL 
                        THEN "Provider Organization Name (Legal Business Name)"
                    WHEN "Provider Last Name (Legal Name)" IS NOT NULL 
                        OR "Provider First Name" IS NOT NULL 
                            THEN TRIM(CONCAT("Provider Name Prefix Text",' ',"Provider First Name", ' ',
                                "Provider Middle Name",  ' ', "Provider Last Name (Legal Name)", ' ',
                                "Provider Name Suffix Text" ,' ', "Provider Credential Text"))
                END AS "Entity Name"
            FROM %I
        ),
        practice_addresses AS (
            SELECT 
                "NPI",
				"Provider Business Practice Location Address Postal Code" as zip_code,
                CASE 
                WHEN "Provider First Line Business Practice Location Address" IS NOT NULL 
                    THEN TRIM(CONCAT(
                        TRIM("Provider First Line Business Practice Location Address"),
                        CASE 
                            WHEN "Provider Second Line Business Practice Location Address" IS NOT NULL 
                            THEN CONCAT(' ', TRIM("Provider Second Line Business Practice Location Address"))
                        END,
                        ', ', TRIM("Provider Business Practice Location Address City Name"),
                        ', ', TRIM("Provider Business Practice Location Address State Name"),
                        ' ', TRIM("Provider Business Practice Location Address Postal Code")
                )) END AS "Practice Address"
            FROM %I
        ),
        taxonomy_codes AS (
            SELECT "NPI",
                CASE 
                    WHEN "Healthcare Provider Primary Taxonomy Switch_1" = 'Y' THEN "Healthcare Provider Taxonomy Code_1"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_2" = 'Y' THEN "Healthcare Provider Taxonomy Code_2"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_3" = 'Y' THEN "Healthcare Provider Taxonomy Code_3"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_4" = 'Y' THEN "Healthcare Provider Taxonomy Code_4"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_5" = 'Y' THEN "Healthcare Provider Taxonomy Code_5"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_6" = 'Y' THEN "Healthcare Provider Taxonomy Code_6"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_7" = 'Y' THEN "Healthcare Provider Taxonomy Code_7"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_8" = 'Y' THEN "Healthcare Provider Taxonomy Code_8"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_9" = 'Y' THEN "Healthcare Provider Taxonomy Code_9"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_10" = 'Y' THEN "Healthcare Provider Taxonomy Code_10"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_11" = 'Y' THEN "Healthcare Provider Taxonomy Code_11"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_12" = 'Y' THEN "Healthcare Provider Taxonomy Code_12"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_13" = 'Y' THEN "Healthcare Provider Taxonomy Code_13"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_14" = 'Y' THEN "Healthcare Provider Taxonomy Code_14"
                    WHEN "Healthcare Provider Primary Taxonomy Switch_15" = 'Y' THEN "Healthcare Provider Taxonomy Code_15"
                END AS "Taxonomy Code"
            FROM %I
        )
        SELECT np."NPI", 
            CASE 
                WHEN CAST(np."Entity Type Code" AS integer) = 1 THEN 'Provider (doctors, nurses, etc.)'
                WHEN CAST(np."Entity Type Code" AS integer) = 2 THEN 'Facility (Hospitals, Urgent Care, Doctors Offices)'
                ELSE NULL 
            END AS "Entity Type", 
            en."Entity Name",
            pa."Practice Address",
            tc."Taxonomy Code",
            nt."Grouping",
            nt."Classification",
            nt."Specialization",
			left(pa.zip_code,5) as zip_code
        FROM %I np
        JOIN entity_names en ON en."NPI" = np."NPI"
        JOIN practice_addresses pa ON pa."NPI" = np."NPI"
        JOIN taxonomy_codes tc ON tc."NPI" = np."NPI"
        JOIN nucc_taxonomy_250 nt ON nt."Code" = tc."Taxonomy Code" AND tc."Taxonomy Code" IS NOT NULL;
    $f$,
    view_name, view_name, src_table_name, src_table_name, src_table_name, src_table_name
    );
END;
$$;

-- If you need to:
-- DROP PROCEDURE create_nppes_csv(text)

CALL create_nppes_csv('nppes_raw');
CALL create_nppes_csv('nppes_sample');


CREATE OR REPLACE PROCEDURE create_nppes_table_with_county()
LANGUAGE plpgsql
AS $$
BEGIN
	DROP TABLE IF EXISTS nppes_summary_with_county;

    CREATE TABLE nppes_summary_with_county AS
		WITH gov_census AS (
			select CONCAT(state, county) as county,
			"B01001_001E" as population
			from gov_census_data
		),
		ranked_counties_by_pop AS (
			SELECT 
			zc."ZIP" as zip_code,
			gc.county,
			gc.population ,
			sf.countyname_fips as county_name,
    		ROW_NUMBER() OVER (PARTITION BY zc."ZIP" ORDER BY population DESC) AS highest_pop_county_by_zip_rank
			FROM gov_census gc
			JOIN zip_county_032025 zc ON zc."COUNTY" = gc.county
			JOIN ssa_fips_state_county_2025 sf ON sf.fipscounty = gc.county
		)
		SELECT nr."NPI",
			nr."Entity Type", 
		    nr."Entity Name",
		    nr."Practice Address",
		    nr."Taxonomy Code",
		    nr."Grouping",
		    nr."Classification",
		    nr."Specialization",
		    nr.zip_code,
			rc.county_name AS county_with_highest_pop
		FROM nppes_raw_summary nr
		JOIN ranked_counties_by_pop rc ON nr.zip_code = rc.zip_code
		WHERE rc.highest_pop_county_by_zip_rank = 1;
END;
$$;

CALL create_nppes_table_with_county();