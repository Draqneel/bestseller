CREATE TABLE bestseller.dim_reviewer (
    reviewer_id text,
    reviewer_name text,
    etl_processed_dttm timestamp,
    PRIMARY KEY (reviewer_id)
);

CREATE TABLE bestseller.dim_item (
    asin text,
    title text,
    price decimal,
    img_url text,
    brand text,
    sales_rank map<text, int>,
    categories list<text>,
    etl_processed_dttm timestamp,
    PRIMARY KEY (asin)
);

CREATE TABLE bestseller.fct_review (
    asin text,
    reviewer_id text,
    review_text text,
    overall double,
    summary text,
    review_ts timestamp,
    review_dt timestamp,
    etl_processed_dttm timestamp,
    PRIMARY KEY ((review_dt), asin, reviewer_id)
);