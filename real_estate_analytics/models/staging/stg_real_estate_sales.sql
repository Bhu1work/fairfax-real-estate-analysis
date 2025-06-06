-- models/staging/stg_real_estate_sales.sql

with source_data as (

    -- Select from the raw source table
    select * from {{ source('raw_data', 'real_estate_sales') }}

)

select
    -- Use the EXACT column names from your screenshot
    "OBJECTID" as property_id,
    "PARID" as parcel_id,
    "TAXYR" as tax_year,
    "PRICE"::numeric as sale_price,

    -- Use "SALEDT" and cast the Unix timestamp to a proper date
    to_timestamp("SALEDT" / 1000) as sale_date,

    "BOOK" as deed_book,
    "PAGE" as deed_page,
    "SALEVAL_DESC" as sale_validation_description

from source_data
WHERE sale_price > 0  -- Remove sales with a price of 0
AND "sale_validation_description" IS NOT NULL -- Remove the (Blank) category