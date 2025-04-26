create table tweet_data
(
    tweet_id   serial primary key,
    xid        bigint,
    username   varchar(50) not null,
    full_name  varchar(50),
    tweet      text        not null,
    url        varchar(100),
    t_views    int         not null default 0,
    t_likes    int         not null default 0,
    t_retweets int         not null default 0,
    created_at TIMESTAMP
);
