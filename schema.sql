create table group_names (
  id integer primary key,
  name varchar not null unique
);

create table artifact_names (
  id integer primary key,
  name varchar not null unique
);

create table group_artifact (
  group_name integer not null, --> group_names(id)
  artifact_name integer not null --> artifact_names(id)
);

create table artifact_version (
  artifact_name integer not null, --> artifact_names(id)
  version_id integer not null --> versions(id)
);

create table versions (
  id integer primary key,
  last_modified timestamp not null,
  size integer,
  source_attached boolean,
  javadoc_attached boolean,
  signature_attached boolean,
  version varchar not null,
  classifier varchar,
  packaging varchar,
  extension varchar,
  name varchar,
  description varchar,
  checksum varchar
);
