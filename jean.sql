CREATE OR REPLACE PROCEDURE create_nppes_csv()
LANGUAGE plpgsql
AS $$
BEGIN
	DROP TABLE IF EXISTS nppes_summary;

	CREATE TABLE nppes_summary AS
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
		FROM nppes_raw
	),
	practice_addresses AS (
		SELECT 
			"NPI",
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
		FROM nppes_raw
	),
	taxonomy_codes AS (
	SELECT "NPI",
		CASE 
			WHEN "Healthcare Provider Primary Taxonomy Switch_1" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_1"
			WHEN "Healthcare Provider Primary Taxonomy Switch_2" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_2"
			WHEN "Healthcare Provider Primary Taxonomy Switch_3" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_3"
			WHEN "Healthcare Provider Primary Taxonomy Switch_4" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_4"
			WHEN "Healthcare Provider Primary Taxonomy Switch_5" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_5"
			WHEN "Healthcare Provider Primary Taxonomy Switch_6" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_6"
			WHEN "Healthcare Provider Primary Taxonomy Switch_7" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_7"
			WHEN "Healthcare Provider Primary Taxonomy Switch_8" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_8"
			WHEN "Healthcare Provider Primary Taxonomy Switch_9" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_9"
			WHEN "Healthcare Provider Primary Taxonomy Switch_10" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_10"
			WHEN "Healthcare Provider Primary Taxonomy Switch_11" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_11"
			WHEN "Healthcare Provider Primary Taxonomy Switch_12" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_12"
			WHEN "Healthcare Provider Primary Taxonomy Switch_13" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_13"
			WHEN "Healthcare Provider Primary Taxonomy Switch_14" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_14"
			WHEN "Healthcare Provider Primary Taxonomy Switch_15" = 'Y' 
				THEN "Healthcare Provider Taxonomy Code_15"
		END AS "Taxonomy Code"
	FROM nppes_raw
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
		nt."Specialization"
	FROM nppes_raw  np
	JOIN entity_names en ON en."NPI" = np."NPI"
	JOIN practice_addresses pa ON pa."NPI" = np."NPI"
	JOIN taxonomy_codes tc ON tc."NPI" = np."NPI"
	JOIN nucc_taxonomy_250 nt ON nt."Code" = tc."Taxonomy Code" AND tc."Taxonomy Code" IS NOT NULL;
END;
$$;

CALL create_nppes_csv();