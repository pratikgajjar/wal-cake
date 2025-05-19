create table if not exists basic (
    id BIGSERIAL PRIMARY KEY,
    created_at timestamp
    with
        time zone default now ()
);

alter table basic replica identity full;
