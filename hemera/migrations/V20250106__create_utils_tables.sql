create table sync_records
(
    mission_sign      varchar not null
primary key,
last_block_number bigint,
update_time       timestamp default now()
);


create table failure_records
(
    record_id          bigserial
        primary key,
    mission_sign       varchar,
    output_types       varchar,
    start_block_number bigint,
    end_block_number   bigint,
    exception_stage    varchar,
    exception          json,
    crash_time         timestamp
);


