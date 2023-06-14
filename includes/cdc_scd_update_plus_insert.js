module.exports = (
    outputTableRef,
    sourceEventsRef,
    uniqueKey,
    timestamp,
    { tags }
) => {
    return operate(`${outputTableRef}_ops`)
        .config({ tags })
        .queries((ctx) => `
            DECLARE last_merged_ts TIMESTAMP 
                DEFAULT (SELECT MAX(scd_valid_from) FROM ${ctx.ref(outputTableRef)});

            CREATE TEMP TABLE events_that_should_be_merged AS (
                SELECT 
                    *
                FROM ${ctx.ref(sourceEventsRef)}
                WHERE (${timestamp} > last_merged_ts) OR (last_merged_ts IS NULL)
            );

            UPDATE ${ctx.ref(outputTableRef)} AS merged
            SET scd_valid_to = new_valid_tos.new_valid_to, is_current = FALSE
            FROM (
                SELECT ${uniqueKey}, MIN(${timestamp}) AS new_valid_to
                FROM events_that_should_be_merged
                GROUP BY ${uniqueKey}
            ) AS new_valid_tos
            WHERE merged.${uniqueKey} = new_valid_tos.${uniqueKey}
                AND merged.is_current IS TRUE;

            INSERT INTO ${ctx.ref(outputTableRef)}
            WITH new_insertions AS (
                SELECT
                    * EXCEPT (${timestamp}),
                    ${timestamp} AS scd_valid_from,
                    LEAD(${timestamp}) OVER 
                        (PARTITION BY ${uniqueKey} ORDER BY ${timestamp} ASC)
                    AS scd_valid_to
                FROM events_that_should_be_merged
            )
            SELECT
                * EXCEPT (operation),
                scd_valid_to IS NULL AS is_current
            FROM new_insertions
            WHERE operation != 'D';
        `);
};
