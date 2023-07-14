create table if not exists rw_qw_customer (
    -- value
    tenent_id string,
    id string,
    "data" jsonb,
    -- key
    company_id string,
    _id string,
    PRIMARY KEY (company_id, _id)
) with (
    connector = 'kafka',
    topic = 'rw_qw_customer',
    properties.bootstrap.server = '127.0.0.1:52821',
    scan.startup.mode = 'earliest'
) FORMAT UPSERT ENCODE JSON;