CREATE OR REPLACE PROCEDURE rename_columns_with_special_chars(table_name_text TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    sql_commands TEXT;
BEGIN
    SELECT string_agg(
        format(
            'ALTER TABLE %I RENAME COLUMN %I TO %I;',
            table_name_text,
            column_name,
            new_column_name
        ),
        E'\n'
    )
    INTO sql_commands
    FROM (
        SELECT 
            column_name,
            LOWER(REGEXP_REPLACE(REPLACE(column_name, E'\n', '_'), '[^a-zA-Z0-9]', '_', 'g')) AS new_column_name
        FROM information_schema.columns
        WHERE table_name = table_name_text
          AND table_schema = 'public'
    ) sub
    WHERE column_name <> new_column_name;

    IF sql_commands IS NOT NULL THEN
        EXECUTE sql_commands;
    END IF;
END;
$$;