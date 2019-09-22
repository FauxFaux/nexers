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

create index versions_last_modified on versions (last_modified);
create index versions_ga on versions (group_id, artifact_id);

-- newest upload of each group/artifact
select (select name from group_names where id = group_id)       as g,
       (select name from artifact_names where id = artifact_id) as a,
       (select version
        from versions v
        where v.group_id = p.group_id
          and v.artifact_id = p.artifact_id
        order by last_modified desc
        limit 1)                                                as v
from (select distinct group_id, artifact_id from versions) p
order by g, a;
