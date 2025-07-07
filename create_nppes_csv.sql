CREATE OR REPLACE PROCEDURE create_nppes_csv(src_table_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    view_name TEXT := src_table_name || '_summary';
BEGIN
    EXECUTE format($f$

        CREATE INDEX IF NOT EXISTS idx_nppes_raw_npi ON nppes_raw(npi);
        CREATE INDEX IF NOT EXISTS idx_nucc_taxonomy_250_code ON nucc_taxonomy_250(code);
        CREATE INDEX IF NOT EXISTS idx_zip ON zip_county_032025(zip);
        CREATE INDEX IF NOT EXISTS idx_nppes_zip ON nppes_raw(provider_business_practice_location_address_postal_code);
        CREATE INDEX IF NOT EXISTS idx_county_zip ON zip_county_032025(county);
        CREATE INDEX IF NOT EXISTS idx_county_ssa ON ssa_fips_state_county_2025(fipscounty);

        CREATE OR REPLACE VIEW %I AS
        WITH entity_names AS (
            SELECT 
                npi,
                CASE 
                    WHEN provider_organization_name__legal_business_name_ IS NOT NULL 
                        THEN provider_organization_name__legal_business_name_
                    WHEN provider_last_name__legal_name_ IS NOT NULL 
                        OR provider_first_name IS NOT NULL 
                            THEN TRIM(CONCAT(
                                provider_name_prefix_text, ' ',
                                provider_first_name, ' ',
                                provider_middle_name, ' ',
                                provider_last_name__legal_name_, ' ',
                                provider_name_suffix_text, ' ',
                                provider_credential_text
                            ))
                END AS entity_name
            FROM %I
        ),
        practice_addresses AS (
            SELECT 
                npi,
                provider_business_practice_location_address_postal_code AS zip_code,
                CASE 
                WHEN provider_first_line_business_practice_location_address IS NOT NULL 
                    THEN TRIM(CONCAT(
                        TRIM(provider_first_line_business_practice_location_address),
                        CASE 
                            WHEN provider_second_line_business_practice_location_address IS NOT NULL 
                            THEN CONCAT(' ', TRIM(provider_second_line_business_practice_location_address))
                        END,
                        ', ', TRIM(provider_business_practice_location_address_city_name),
                        ', ', TRIM(provider_business_practice_location_address_state_name),
                        ' ', TRIM(provider_business_practice_location_address_postal_code)
                )) END AS practice_address
            FROM %I
        ),
        taxonomy_codes AS (
            SELECT npi,
                CASE 
                    WHEN healthcare_provider_primary_taxonomy_switch_1 = 'Y' THEN healthcare_provider_taxonomy_code_1
                    WHEN healthcare_provider_primary_taxonomy_switch_2 = 'Y' THEN healthcare_provider_taxonomy_code_2
                    WHEN healthcare_provider_primary_taxonomy_switch_3 = 'Y' THEN healthcare_provider_taxonomy_code_3
                    WHEN healthcare_provider_primary_taxonomy_switch_4 = 'Y' THEN healthcare_provider_taxonomy_code_4
                    WHEN healthcare_provider_primary_taxonomy_switch_5 = 'Y' THEN healthcare_provider_taxonomy_code_5
                    WHEN healthcare_provider_primary_taxonomy_switch_6 = 'Y' THEN healthcare_provider_taxonomy_code_6
                    WHEN healthcare_provider_primary_taxonomy_switch_7 = 'Y' THEN healthcare_provider_taxonomy_code_7
                    WHEN healthcare_provider_primary_taxonomy_switch_8 = 'Y' THEN healthcare_provider_taxonomy_code_8
                    WHEN healthcare_provider_primary_taxonomy_switch_9 = 'Y' THEN healthcare_provider_taxonomy_code_9
                    WHEN healthcare_provider_primary_taxonomy_switch_10 = 'Y' THEN healthcare_provider_taxonomy_code_10
                    WHEN healthcare_provider_primary_taxonomy_switch_11 = 'Y' THEN healthcare_provider_taxonomy_code_11
                    WHEN healthcare_provider_primary_taxonomy_switch_12 = 'Y' THEN healthcare_provider_taxonomy_code_12
                    WHEN healthcare_provider_primary_taxonomy_switch_13 = 'Y' THEN healthcare_provider_taxonomy_code_13
                    WHEN healthcare_provider_primary_taxonomy_switch_14 = 'Y' THEN healthcare_provider_taxonomy_code_14
                    WHEN healthcare_provider_primary_taxonomy_switch_15 = 'Y' THEN healthcare_provider_taxonomy_code_15
                END AS taxonomy_code
            FROM %I
        )
        SELECT np.npi, 
            CASE 
                WHEN CAST(np.entity_type_code AS integer) = 1 THEN 'Provider (doctors, nurses, etc.)'
                WHEN CAST(np.entity_type_code AS integer) = 2 THEN 'Facility (Hospitals, Urgent Care, Doctors Offices)'
                ELSE NULL 
            END AS entity_type, 
            en.entity_name,
            pa.practice_address,
            tc.taxonomy_code,
            nt.grouping,
            nt.classification,
            nt.specialization,
            left(pa.zip_code,5) as zip_code
        FROM %I np
        JOIN entity_names en ON en.npi = np.npi
        JOIN practice_addresses pa ON pa.npi = np.npi
        JOIN taxonomy_codes tc ON tc.npi = np.npi
        JOIN nucc_taxonomy_250 nt ON nt.code = tc.taxonomy_code AND tc.taxonomy_code IS NOT NULL;
    $f$,
    view_name, src_table_name, src_table_name, src_table_name, src_table_name
    );
END;
$$;
