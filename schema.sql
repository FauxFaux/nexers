create table versions (
  id integer primary key,
  group_id integer not null,
  artifact_id integer not null,
  version varchar not null,
  classifier_id integer,
  extension_id integer,

  packaging_id integer,

  last_modified timestamp not null,
  size integer,
  checksum varchar,

  source_attached boolean,
  javadoc_attached boolean,
  signature_attached boolean,

  name_id integer,
  desc_id integer
);
