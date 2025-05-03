create table articles
(
    id              serial primary key,
    title           varchar(50) not null,
    url             text        not null,
    author          varchar(50),
    crawl_status    varchar(20) not null default 'PENDING',
    priority        int,
    retries         int                  default 1,
    last_crawled_at timestamp
);

create table crawled_pages
(
    id              serial primary key,
    parent_id       int references articles (id),
    url             text        not null unique,
    title           text,
    crawl_status    varchar(20) not null,
    last_crawled_at timestamp,
    content_hash    varchar(255)
);
