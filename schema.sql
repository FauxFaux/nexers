create table group_names (
  id integer primary key,
  name varchar not null unique
);

create table artifact_names (
  id integer primary key,
  name varchar not null unique
);

create table name_names (
  id integer primary key,
  name varchar not null unique
);

create table desc_names (
  id integer primary key,
  name varchar not null unique
);

create table versions (
  id integer primary key,
  group_id integer not null,
  artifact_id integer not null,
  last_modified timestamp not null,
  size integer,
  source_attached boolean,
  javadoc_attached boolean,
  signature_attached boolean,
  name_id integer,
  desc_id integer,
  version varchar not null,
  classifier varchar,
  packaging varchar,
  extension varchar,
  checksum varchar
);
