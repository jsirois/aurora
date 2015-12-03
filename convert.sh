#!/usr/bin/env bash

git grep -E "org\.apache\.aurora\.scheduler\.storage\.entities" -- src/ | \
  cut -d: -f1 | \
  sort -u | \
  xargs sed -ri -e "s|org\.apache\.aurora\.scheduler\.storage\.entities\.I|org.apache.aurora.gen.|g"

while read entity
do
  hits=$(git grep -E "\b${entity}\b" -- src/ | cut -d: -f1 | sort -u)
  if [[ -n "${hits}" ]]
  then
  echo "Converting ${entity} -> ${entity#I}"
    echo ${hits} | xargs sed -ri -e "s|\b${entity}\b|${entity#I}|g"
  else
    echo "Skipping ${entity} - no usages"
  fi
done < $HOME/desktop/org.apache.aurora.scheduler.storage.entities-thriftEntities.names.txt

git grep -E "(api|storage)Constants" -- src/ | \
  cut -d: -f1 | \
  sort -u | \
  xargs sed -ri -e "s/(api|storage)Constants/Constants/g"

git grep -E "\.Iface" -- src/ | \
  cut -d: -f1 | \
  sort -u | \
  xargs sed -ri -e "s|\.Iface|.Sync|g"

