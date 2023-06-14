module.exports = (
    outputTableRef,  // The Dataform reference for the output table
    sourceEventsRef,  // The Dataform reference for the source table
    uniqueKey,  // The unique key column for the source table
    timestamp,  // The timestamp column detailing the timestamp for each event
    { tags }
) => {
    return publish(`${outputTableRef}`)
        .type("incremental")
        .query((ctx) => `
            WITH events AS (
                -- Limit the events processed to those that happened after the last
                -- known merge timestamp for incremental runs.
                SELECT * FROM ${ctx.ref(sourceEventsRef)}
                ${ctx.when(ctx.incremental(), `
                    WHERE ${timestamp} >= (SELECT MAX(COALESCE(scd_valid_to, scd_valid_from)) FROM ${ctx.self()})
                `)}
                -- Remove possible duplicates
                QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY ${uniqueKey}, ${timestamp} ORDER BY ${timestamp})
            ),
            -- When in incremental mode, fetch all current records
            -- These are the ones that could be updated, but only the ones
            -- that had changes will be
            ${ctx.when(ctx.incremental(), `
            current_data AS (
                SELECT * FROM ${ctx.self()} WHERE is_current IS TRUE
            ),
            `)}
            incremental_data AS (
                SELECT
                    events.* EXCEPT (${timestamp}),
                    events.${timestamp} AS scd_valid_from,
                    LEAD(events.${timestamp}) OVER 
                        (PARTITION BY events.${uniqueKey} ORDER BY events.${timestamp} ASC)
                    AS scd_valid_to
                FROM
                    events
                -- If running in incremental mode, upsert only the records that either
                --   A. are current and had at least one new event. These will be closed.
                --   B. are new events. If there are multiple new events for the same key, the
                --      ones that should be closed will be.
                ${ctx.when(ctx.incremental(), `
                    LEFT JOIN current_data AS merged 
                        ON events.${uniqueKey} = merged.${uniqueKey}
                        AND events.${timestamp} = merged.scd_valid_from
                `)}
            )
            SELECT
                * EXCEPT (operation),
                scd_valid_to IS NULL AS is_current
            FROM incremental_data
            -- Deletes should not be inserted as separate rows
            -- Previous records will be closed accordingly anyway, because their
            -- valid_to date was set in the incremental_data CTE
            WHERE operation != 'D'
        `)
        .tags(tags)
        .uniqueKey([uniqueKey, "scd_valid_from"]);
}