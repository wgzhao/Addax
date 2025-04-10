create table default.hive_reader
(
    col1 int,
    col2 string,
    col3 timestamp
)
stored as orc;


insert into hive_reader values(1, 'hello', current_timestamp()), (2, 'world', current_timestamp());