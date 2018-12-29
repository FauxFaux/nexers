create table group_names (
  id integer primary key,
  name varchar not null unique
);

create table artifact_names (
  id integer primary key,
  name varchar not null unique
);

create table full_descriptions (
  id integer primary key,
  name varchar not null,
  description varchar not null,
  unique (name, description)
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
  name_desc_id integer not null,
  version varchar not null,
  classifier varchar,
  packaging varchar,
  extension varchar,
  checksum varchar
);
