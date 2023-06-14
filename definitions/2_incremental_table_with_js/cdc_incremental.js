cdc_scd_incremental_table(
    /*outputTableRef=*/"cdc_incremental_js",
    /*sourceTableRef=*/"transformed_events",
    /*uniqueKey=*/"key",
    /*timestampColumn=*/"timestamp",
    {
        tags: []
    }
);
