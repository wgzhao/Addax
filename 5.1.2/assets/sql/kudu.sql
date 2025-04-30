CREATE TABLE kudu.default.users (
  user_id int WITH (primary_key = true),
  user_name varchar with (nullable=true),
  age int with (nullable=true),
  salary double with (nullable=true),
  longtitue decimal(18,6) with (nullable=true),
  latitude decimal(18,6) with (nullable=true),
  p decimal(21,20) with (nullable=true),
  mtime timestamp with (nullable=true)
) WITH (
  partition_by_hash_columns = ARRAY['user_id'],
  partition_by_hash_buckets = 2
);

insert into kudu.default.users 
values 
(1, cast('wgzhao' as varchar), 18, cast(18888.88 as double), 
 cast(123.282424 as decimal(18,6)), cast(23.123456 as decimal(18,6)),
 cast(1.12345678912345678912 as decimal(21,20)), 
 timestamp '2021-01-10 14:40:41'),
(2, cast('anglina' as varchar), 16, cast(23456.12 as double), 
 cast(33.192123 as decimal(18,6)), cast(56.654321 as decimal(18,6)), 
 cast(1.12345678912345678912 as decimal(21,20)), 
 timestamp '2021-01-10 03:40:41');
-- ONLY insert primary key value
 insert into kudu.default.users(user_id) values  (3);