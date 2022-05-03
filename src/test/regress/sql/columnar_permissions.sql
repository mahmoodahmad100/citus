
select current_user \gset

create table no_access(i int) using columnar;
insert into no_access values(1);
insert into no_access values(2);
insert into no_access values(3);

-- only owner can access
select alter_columnar_table_reset('no_access'::regclass, compression=>true);
select alter_columnar_table_set('no_access'::regclass, compression=>'none');
select 1 from columnar.get_storage_id('no_access'::regclass);
select relation, chunk_group_row_limit, stripe_row_limit, compression, compression_level
  from columnar.options where relation='no_access'::regclass;

create user columnar_user;

\c - columnar_user

-- errors
select alter_columnar_table_reset('no_access'::regclass, compression=>true);
select alter_columnar_table_set('no_access'::regclass, compression=>'zstd');
select 1 from columnar.get_storage_id('no_access'::regclass);

-- empty because columnar_user doesn't have ownership privileges
-- on 'no_access'
select relation, chunk_group_row_limit, stripe_row_limit, compression, compression_level
  from columnar.options where relation='no_access'::regclass;

create table columnar_permissions(i int) using columnar;
insert into columnar_permissions values(1);
alter table columnar_permissions add column j int;
insert into columnar_permissions values(2,20);
vacuum columnar_permissions;
truncate columnar_permissions;
drop table columnar_permissions;

\c - :current_user

drop table no_access;
