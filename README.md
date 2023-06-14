# Example Dataform project for implementing CDC + SCD2 on BigQuery

This is an example project showcasing two approaches to merge CDC events into a BigQuery table using SCD Type 2 (Slowly Changing Dimensions).

## Input

The project assumes there's an existing table containing CDC events with the following schema:

| key | timestamp           | operation | columnA | columnB |
|-----|---------------------|-----------|---------|---------|
| 1   | 2023-05-10 00:00:00 | I         | data    | data    |
| 1   | 2023-05-10 00:01:00 | U         | updated | updated |
| 1   | 2023-05-10 00:02:00 | D         |         |         |
| 2   | 2023-05-10 00:03:00 | I         | other   | other   |

Where:

* `key` is the unique key for each record
* `timestamp` is the timestamp for each CDC event
* `operation` is either inserted (I), updated (U) or deleted (D)
* `columnA` and `columnB` are data columns

## Output

Each of the approaches produces a table with the following schema:

| key | columnA | columnB | scd_valid_from      | scd_valid_to        | is_current |
|-----|---------|---------|---------------------|---------------------|------------|
| 1   | data    | data    | 2023-05-10 00:00:00 | 2023-05-10 00:01:00 | FALSE      |
| 1   | updated | updated | 2023-05-10 00:01:00 | 2023-05-10 00:02:00 | FALSE      |
| 2   | other   | other   | 2023-05-10 00:03:00 | NULL                | TRUE       |

Notice how:

* The object with key 1 and data columns (`data`, `data`) was current from 00:00 hs to 01:00 hs. This is because it was inserted at 00:00 hs and updated at 01:00 hs. It is no longer current.
* The object with key 1 and data columns (`updated`, `updated`) was current from 01:00 hs to 02:00 hs. This is because it was updated at 01:00 hs and deleted at 02:00 hs. It is no longer current.
* The object with key 2 and data columns (`other`, `other`) was inserted at 03:00 hs and is current. This is because it was inserted at 03:00 hs and has not been updated or deleted.

## First approach: using an incremental table

The first proposed approach is to implement the merge process with an [incremental table](https://docs.dataform.co/guides/datasets/incremental).

The project ships two implementations of it:
* [The first one](./definitions/1_incremental_table) writes the SQLX for the incremental table manually, explaining each of the steps
* [The second one](./definitions/2_incremental_table_with_js) generalizes the pattern using Javascript, so that it is easily reproducible for many tables. The corresponding Javascript file can be found at [`includes/cdc_scd_incremental_table.js`](./includes/cdc_scd_incremental_table.js).

## Second approach: using an UPDATE plus an INSERT statement

The second proposed approach is to implement the merge process using an UPDATE  statment plus an INSERT statement.

The project ships two implementations of it:
* [The first one](./definitions/3_update_plus_insert) writes the SQLX for the process manually, explaining each of the steps.
* [The second one](./definitions/4_update_plus_insert_with_js) generalizes the pattern using Javascript, so that it is easily reproducible for many tables. The corresponding Javascript file can be found at [`includes/cdc_scd_update_plus_insert.js`](./includes/cdc_scd_update_plus_insert.js).

## Testing the solution

#### 1. Configure your project ID

Edit the [`dataform.json`](./dataform.json) file and set the `defaultDatabase` to your BigQuery project id.

#### 2. Create initial testing events table

Set the project id accordingly.

``` sql
CREATE SCHEMA `PROJECT_ID.cdc_scd`;

CREATE OR REPLACE TABLE `PROJECT_ID.cdc_scd.events` AS (
  SELECT 1 AS `key`, TIMESTAMP("2023-05-10 00:00:00+00") AS `timestamp`, 
    'I' AS operation, DATE('2023-06-10') AS columnA, 'Hi' AS columnB
  UNION ALL
  SELECT 1 AS `key`, TIMESTAMP("2023-05-10 00:01:00+00") AS `timestamp`,
    'U' AS operation, DATE('2023-06-10') AS columnA, 'Bye' AS columnB
  UNION ALL
  SELECT 1 AS `key`, TIMESTAMP("2023-05-10 00:02:00+00") AS `timestamp`,
    'D' AS operation, DATE('2023-06-11') AS columnA, 'Bye' AS columnB
  UNION ALL
  SELECT 2 AS `key`, TIMESTAMP("2023-05-10 00:03:00+00") AS `timestamp`,
    'I' AS operation, DATE('2023-06-11') AS columnA, 'Value' AS columnB
);
```

#### 3. Run the Dataform project 

``` bash
dataform init-creds bigquery
npm install
dataform run
```

#### 4. Validate the created CDC tables

The project will create multiple tables to account for the different approaches presented. All the created tables are:
* `PROJECT_ID.cdc_scd.cdc_incremental`
* `PROJECT_ID.cdc_scd.cdc_incremental_js`
* `PROJECT_ID.cdc_scd.cdc_update_plus_insert`
* `PROJECT_ID.cdc_scd.cdc_update_plus_insert_js`

Any of those tables will have the merged records with the format shown above.

#### 5. Insert new events and test incremental loads

First insert new events into the source events table:

``` sql
INSERT INTO `PROJECT_ID.cdc_scd.events`(`key`, `timestamp`, operation, columnA, columnB) VALUES
  (2, TIMESTAMP("2023-06-10 00:04:00+00"), 'U', DATE('2023-06-11'), 'Value 2'),
  (2, TIMESTAMP("2023-06-10 00:05:00+00"), 'U', DATE('2023-06-11'), 'Value 3');
```

Then run the Dataform project using `dataform run` and validate the tables have been updated as expected.
