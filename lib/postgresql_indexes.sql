SELECT
  ix.relname as index_name,
  indisunique as is_unique,
  replace(regexp_replace(regexp_replace(pg_get_indexdef(indexrelid), ' WHERE .+', ''), '.*\((.*)\)', '\1'), ' ', '') as column_name

FROM pg_index i
JOIN pg_class t ON t.oid = i.indrelid
JOIN pg_class ix ON ix.oid = i.indexrelid
JOIN pg_namespace n ON t.relnamespace = n.oid
WHERE t.relname = $1;
