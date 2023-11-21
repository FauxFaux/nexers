#!/bin/zsh
set -eu

REF=$1

for n in group artifact name desc classifier; do
  echo "select (select name from ${n}_names where id=fid) name \
        from (select ${n}_id fid,count(*) cnt \
          from versions where fid is not null \
          group by ${n}_id\
        )\
        where name not like '%' || char(10) || '%'
        order by cnt desc limit 256" \
    | sqlite3 ${REF} > top_${n}.txt
done

# both packaging and extension use the packaging table
echo "select (select name from packaging_names where id=fid) \
      from (select fid,count(*) cnt from (\
        select packaging_id fid from versions \
          union all select extension_id fid from versions\
      ) where fid is not null group by fid\
      ) order by cnt desc limit 256" \
    | sqlite3 ${REF} > top_packaging.txt
