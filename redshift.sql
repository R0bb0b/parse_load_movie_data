begin transaction;

DROP TABLE IF EXISTS staging_collections cascade;
DROP TABLE IF EXISTS staging_credits cascade;
DROP TABLE IF EXISTS staging_genres cascade;
DROP TABLE IF EXISTS staging_keywords cascade;
DROP TABLE IF EXISTS staging_languages cascade;
DROP TABLE IF EXISTS staging_links cascade;
DROP TABLE IF EXISTS staging_movies_metadata cascade;
DROP TABLE IF EXISTS staging_movies_metadata_genres cascade;
DROP TABLE IF EXISTS staging_movies_metadata_keywords cascade;
DROP TABLE IF EXISTS staging_movies_metadata_languages cascade;
DROP TABLE IF EXISTS staging_movies_metadata_production_companies cascade;
DROP TABLE IF EXISTS staging_movies_metadata_production_countries cascade;
DROP TABLE IF EXISTS staging_production_companies cascade;
DROP TABLE IF EXISTS staging_production_countries cascade;
DROP TABLE IF EXISTS staging_ratings cascade;

DROP TABLE IF EXISTS collections cascade;
DROP TABLE IF EXISTS credits cascade;
DROP TABLE IF EXISTS genres cascade;
DROP TABLE IF EXISTS keywords cascade;
DROP TABLE IF EXISTS languages cascade;
DROP TABLE IF EXISTS links cascade;
DROP TABLE IF EXISTS movies_metadata cascade;
DROP TABLE IF EXISTS movies_metadata_genres cascade;
DROP TABLE IF EXISTS movies_metadata_keywords cascade;
DROP TABLE IF EXISTS movies_metadata_languages cascade;
DROP TABLE IF EXISTS movies_metadata_production_companies cascade;
DROP TABLE IF EXISTS movies_metadata_production_countries cascade;
DROP TABLE IF EXISTS production_companies cascade;
DROP TABLE IF EXISTS production_countries cascade;
DROP TABLE IF EXISTS ratings cascade;

CREATE TABLE collections (
    collection_id           integer     	not null	sortkey distkey,
    name                    varchar(150) 	not null,
    poster_path             varchar(50) 	not null,
    backdrop_path           varchar(50) 	not null
);

CREATE TABLE credits (
    tmdb_id                 integer     	not null	sortkey distkey,
    cast_id                 integer,
    character               varchar(max),
    credit_id               varchar(50)     not null,
    department              varchar(20),
    gender                  integer,
    contributor_id          integer     	not null,
    job                     varchar(100)    not null,
    name                    varchar(150)    not null,
    cast_order              integer,
    profile_path            varchar(50)
);

CREATE TABLE genres (
    genre_id                integer     	not null	sortkey distkey,
    name                    varchar(50)     not null
);

CREATE TABLE keywords (
    keyword_id              integer     	not null	sortkey distkey,
    keyword                 varchar(100)    not null
);

CREATE TABLE languages (
    language_abbr           char(2)     	not null	sortkey distkey,
    language                varchar(100)    not null
);

CREATE TABLE links (
    movie_id                integer     	not null	sortkey distkey,
    tmdb_id                 integer         not null,
    imdb_id                 integer
);

CREATE TABLE movies_metadata (
    adult                   boolean,
    collection_id           integer         sortkey distkey,
    budget                  integer,
    homepage                text,
    tmdb_id                 integer         not null,
    imdb_id                 integer,
    original_language       char(2),
    original_title          varchar(150),
    overview                varchar(max),
    popularity              decimal(15,10),
    poster_path             varchar(50),
    release_date            date,
    revenue                 bigint,
    runtime                 integer,
    status                  varchar(20),
    tagline                 varchar(max),
    title                   text,
    video                   boolean,
    vote_average            decimal(3,1),
    vote_count              integer
);

CREATE TABLE movies_metadata_genres (
    tmdb_id                 integer     	not null	sortkey distkey,
    genre_id                integer         not null
);

CREATE TABLE movies_metadata_keywords (
    tmdb_id                 integer     	not null	sortkey distkey,
    keyword_id              integer         not null
);

CREATE TABLE movies_metadata_languages (
    tmdb_id                 integer     	not null	sortkey distkey,
    language_abbr           char(2)         not null
);

CREATE TABLE movies_metadata_production_companies (
    tmdb_id                 integer     	not null	sortkey distkey,
    production_company_id   integer         not null
);

CREATE TABLE movies_metadata_production_countries (
    tmdb_id                 integer     	not null	sortkey distkey,
    production_country      char(2)         not null
);

CREATE TABLE production_companies (
    production_company_id   integer     	not null	sortkey distkey,
    name                    varchar(150)    not null
);

CREATE TABLE production_countries (
    production_country      char(2)     	not null	sortkey distkey,
    name                    varchar(50)     not null
);

CREATE TABLE ratings (
    movie_id                integer         not null sortkey distkey,
    user_id                 integer,
    rating                  decimal(3,1),
    "timestamp"             timestamp
);

commit transaction;
