create view vers as
select id,
       (select name from group_names where id = group_id)           as `group`,
       (select name from artifact_names where id = artifact_id)     as artifact,
       version,
       (select name from classifier_names where id = classifier_id) as classifier,
       (select name from packaging_names where id = packaging_id)   as packaging,
       (select name from packaging_names where id = extension_id)   as extension,
       datetime(last_modified, 'unixepoch')                         as last_modified,
       size,
       checksum,
       source_attached,
       javadoc_attached,
       signature_attached,
       name_id,
       desc_id
from versions;
