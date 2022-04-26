--创建数据库并切换使用
create database if not exists itheima;
use itheima;

drop table t_archer;
--ddl create table
create table t_archer(
      id int comment "ID",
      name string comment "英雄名称",
      hp_max int comment "最大生命",
      mp_max int comment "最大法力",
      attack_max int comment "最高物攻",
      defense_max int comment "最大物防",
      attack_range string comment "攻击范围",
      role_main string comment "主要定位",
      role_assist string comment "次要定位"
) comment "王者荣耀射手信息"
row format delimited
fields terminated by "\t";

select *
from t_archer;

desc formatted t_archer;


create table t_hot_hero_skin_price(
      id int,
      name string,
      win_rate int,
      skin_price map<string,int>
)
row format delimited
fields terminated by ',' --字段之间分隔符
collection items terminated by '-'  --集合元素之间分隔符
map keys terminated by ':'; --集合元素kv之间分隔符;


select *
from t_hot_hero_skin_price;

create table t_team_ace_player(
   id int,
   team_name string,
   ace_player_name string
); --没有指定row format语句 此时采用的是默认的\001作为字段的分隔符

select * from t_team_ace_player;



create table t_team_ace_player_location(
 id int,
 team_name string,
 ace_player_name string)
 location '/data'; --使用location关键字指定本张表数据在hdfs上的存储路径

select * from t_team_ace_player_location;

------------------------------------


--默认情况下 创建的表就是内部表
create table student(
     num int,
     name string,
     sex string,
     age int,
     dept string)
row format delimited
fields terminated by ',';

--可以使用DESCRIBE FORMATTED itheima.student
-- 来获取表的描述信息，从中可以看出表的类型。
describe formatted itheima.student;

--创建外部表 需要关键字 external
--外部表数据存储路径不指定 默认规则和内部表一致
--也可以使用location关键字指定HDFS任意路径
create external table student_ext(
   num int,
   name string,
   sex string,
   age int,
   dept string)
row format delimited
fields terminated by ','
location '/stu';

--创建外部表 不指定location
create external table student_ext_nolocation(
                                     num int,
                                     name string,
                                     sex string,
                                     age int,
                                     dept string)
    row format delimited
        fields terminated by ',';

drop table  student_ext_nolocation;

describe formatted itheima.student_ext;


show tables;

select *
from student_ext;

select *
from student;


drop table student;
drop table student_ext;
show tables;


create table t_all_hero(
   id int,
   name string,
   hp_max int,
   mp_max int,
   attack_max int,
   defense_max int,
   attack_range string,
   role_main string,
   role_assist string
)
row format delimited
fields terminated by "\t";

--hadoop fs -put archer.txt assassin.txt mage.txt support.txt tank.txt warrior.txt /user/hive/warehouse/itheima.db/t_all_hero

select * from t_all_hero;


--查询role_main主要定位是射手并且hp_max最大生命大于6000的有几个
select count(*) from t_all_hero where role_main="archer" and hp_max >6000;



--注意分区表创建语法规则
--分区表建表
create table t_all_hero_part(
   id int,
   name string,
   hp_max int,
   mp_max int,
   attack_max int,
   defense_max int,
   attack_range string,
   role_main string,
   role_assist string
) partitioned by (role string)--注意哦 这里是分区字段
row format delimited
fields terminated by "\t";


select * from t_all_hero_part;

--静态加载分区表数据
load data local inpath '/opt/data/honor/hero/archer.txt' into table t_all_hero_part partition(role='sheshou');
load data local inpath '/opt/data/honor/hero/assassin.txt' into table t_all_hero_part partition(role='cike');
load data local inpath '/opt/data/honor/hero/mage.txt' into table t_all_hero_part partition(role='fashi');
load data local inpath '/opt/data/honor/hero/support.txt' into table t_all_hero_part partition(role='fuzhu');
load data local inpath '/opt/data/honor/hero/tank.txt' into table t_all_hero_part partition(role='tanke');
load data local inpath '/opt/data/honor/hero/warrior.txt' into table t_all_hero_part partition(role='zhanshi');


--非分区表 全表扫描过滤查询
select count(*) from t_all_hero where role_main="archer" and hp_max >6000;
--分区表 先基于分区过滤 再查询
select count(*) from t_all_hero_part where role="sheshou" and hp_max >6000;

-----多重分区表
--单分区表，按省份分区
create table t_user_province (id int, name string,age int) partitioned by (province string);
--双分区表，按省份和市分区
--分区字段之间是一种递进的关系 因此要注意分区字段的顺序 谁在前在后
create table t_user_province_city (id int, name string,age int) partitioned by (province string, city string);

--双分区表的数据加载 静态分区加载数据
-- load data local inpath '/root/hivedata/user.txt' into table t_user_province_city
--     partition(province='zhejiang',city='hangzhou');
-- load data local inpath '/root/hivedata/user.txt' into table t_user_province_city
--     partition(province='zhejiang',city='ningbo');
-- load data local inpath '/root/hivedata/user.txt' into table t_user_province_city
--     partition(province='shanghai',city='pudong');

--双分区表的使用  使用分区进行过滤 减少全表扫描 提高查询效率
select * from t_user_province_city where  province= "zhejiang" and city ="hangzhou";

--动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--创建一张新的分区表 t_all_hero_part_dynamic
create table t_all_hero_part_dynamic(
    id int,
    name string,
    hp_max int,
    mp_max int,
    attack_max int,
    defense_max int,
    attack_range string,
    role_main string,
    role_assist string
) partitioned by (role string)
row format delimited
fields terminated by "\t";


select * from t_all_hero;

--执行动态分区插入
insert into table t_all_hero_part_dynamic partition(role) --注意这里 分区值并没有手动写死指定
select tmp.*,tmp.role_main from t_all_hero tmp;

select * from t_all_hero_part_dynamic;


--单分区表，按省份分区
create table t_user_province (id int, name string,age int) partitioned by (province string);

--双分区表，按省份和市分区
create table t_user_province_city (id int, name string,age int) partitioned by (province string, city string);

--三分区表，按省份、市、县分区
create table t_user_province_city_county (id int, name string,age int) partitioned by (province string, city string,county string);

--多分区表的数据插入和查询使用
-- load data local inpath '/root/hivedata/user.txt' into table t_user_province partition(province='shanghai');

-- load data local inpath '/root/hivedata/user.txt' into table t_user_province_city_county partition(province='zhejiang',city='hangzhou',county='xiaoshan');

select * from t_user_province_city_county where province='zhejiang' and city='hangzhou';


--分桶表建表语句
-- CREATE [EXTERNAL] TABLE [db_name.]table_name
-- [(col_name data_type, ...)]
-- CLUSTERED BY (col_name)
-- INTO N BUCKETS;


CREATE TABLE t_usa_covid19_bucket(
      count_date string,
      county string,
      state string,
      fips int,
      cases int,
      deaths int)
CLUSTERED BY(state) INTO 5 BUCKETS; --分桶的字段一定要是表中已经存在的字段


--根据state州分为5桶 每个桶内根据cases确诊病例数倒序排序
CREATE TABLE t_usa_covid19_bucket_sort(
     count_date string,
     county string,
     state string,
     fips int,
     cases int,
     deaths int)
CLUSTERED BY(state)
sorted by (cases desc) INTO 5 BUCKETS;--指定每个分桶内部根据 cases倒序排序


--step1:开启分桶的功能 从Hive2.0开始不再需要设置
set hive.enforce.bucketing=true;

--step2:把源数据加载到普通hive表中
CREATE TABLE  if not exists itheima.t_usa_covid19(
       count_date string,
       county string,
       state string,
       fips int,
       cases int,
       deaths int)
row format delimited fields terminated by ",";

--将源数据上传到HDFS，t_usa_covid19表对应的路径下
-- hadoop fs -put us-covid19-counties.dat /user/hive/warehouse/itheima.db/t_usa_covid19

--step3:使用insert+select语法将数据加载到分桶表中
insert into t_usa_covid19_bucket select * from t_usa_covid19;

select * from t_usa_covid19_bucket;

--基于分桶字段state查询来自于New York州的数据
--不再需要进行全表扫描过滤
--根据分桶的规则hash_function(New York) mod 5计算出分桶编号
--查询指定分桶里面的数据 就可以找出结果  此时是分桶扫描而不是全表扫描
select *
from t_usa_covid19_bucket where state="New York";

select *
from student;

---Hive事务表
--Step1：创建普通的表
drop table if exists itheima.student;
create table student(
    num int,
    name string,
    sex string,
    age int,
    dept string)
row format delimited
fields terminated by ',';
--Step2：加载数据到普通表中
load data local inpath '/root/hivedata/students.txt' into table itheima.student;
select * from itheima.student;

--Step3：执行更新操作
update student
set age = 66
where num = 95001;


--Hive中事务表的创建使用
--1、开启事务配置（可以使用set设置当前session生效 也可以配置在hive-site.xml中）
set hive.support.concurrency = true; --Hive是否支持并发
set hive.enforce.bucketing = true; --从Hive2.0开始不再需要  是否开启分桶功能
set hive.exec.dynamic.partition.mode = nonstrict; --动态分区模式  非严格
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; --
set hive.compactor.initiator.on = true; --是否在Metastore实例上运行启动线程和清理线程
set hive.compactor.worker.threads = 1; --在此metastore实例上运行多少个压缩程序工作线程。

--2、创建Hive事务表
drop table if exists trans_student;
create table trans_student(
   id int,
   name String,
   age int
)clustered by (id) into 2 buckets stored as orc TBLPROPERTIES('transactional'='true');
--注意 事务表创建几个要素：开启参数、分桶表、存储格式orc、表属性

--3、针对事务表进行insert update delete操作
insert into trans_student values(1,"allen",18);

update trans_student
set age = 20
where id = 1;

delete from trans_student where id =1;

select *
from trans_student;




---Hive View视图相关语法
--hive中有一张真实的基础表t_usa_covid19
select *
from itheima.t_usa_covid19;

--1、创建视图
create view v_usa_covid19 as select count_date, county,state,deaths from t_usa_covid19 limit 5;

--能否从已有的视图中创建视图呢  可以的
create view v_usa_covid19_from_view as select * from v_usa_covid19 limit 2;

--2、显示当前已有的视图
show tables;
show views;--hive v2.2.0之后支持

--3、视图的查询使用
select *
from v_usa_covid19;

--能否插入数据到视图中呢？
--不行 报错  SemanticException:A view cannot be used as target table for LOAD or INSERT
insert into v_usa_covid19 select count_date,county,state,deaths from t_usa_covid19;

--4、查看视图定义
show create table v_usa_covid19;

--5、删除视图
drop view v_usa_covid19_from_view;
--6、更改视图属性
alter view v_usa_covid19 set TBLPROPERTIES ('comment' = 'This is a view');
--7、更改视图定义
alter view v_usa_covid19 as  select county,deaths from t_usa_covid19 limit 2;



--通过视图来限制数据访问可以用来保护信息不被随意查询:
create table userinfo(firstname string, lastname string, ssn string, password string);

create view safer_user_info as select firstname, lastname from userinfo;

--可以通过where子句限制数据访问，比如，提供一个员工表视图，只暴露来自特定部门的员工信息:
create table employee(firstname string, lastname string, ssn string, password string, department string);

create view techops_employee as select firstname, lastname, ssn from userinfo where department = 'java';


--使用视图优化嵌套查询
from (
         select * from people join cart
                                   on(cart.pepople_id = people.id) where firstname = 'join'
     )a select a.lastname where a.id = 3;

--把嵌套子查询变成一个视图
create view shorter_join as
select * from people join cart
                          on (cart.pepople_id = people.id) where firstname = 'join';
--基于视图查询
select lastname from shorter_join where id = 3;





---Hive 物化视图------------------------------
-- Drops a materialized view
-- DROP MATERIALIZED VIEW [db_name.]materialized_view_name;
-- Shows materialized views (with optional filters)
-- SHOW MATERIALIZED VIEWS [IN database_name];
-- Shows information about a specific materialized view
-- DESCRIBE [EXTENDED | FORMATTED] [db_name.]materialized_view_name;

-- ALTER MATERIALIZED VIEW [db_name.]materialized_view_name REBUILD;
-- ALTER MATERIALIZED VIEW [db_name.]materialized_view_name ENABLE|DISABLE REWRITE;


--1、新建一张事务表 student_trans
set hive.support.concurrency = true; --Hive是否支持并发
set hive.enforce.bucketing = true; --从Hive2.0开始不再需要  是否开启分桶功能
set hive.exec.dynamic.partition.mode = nonstrict; --动态分区模式  非严格
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; --
set hive.compactor.initiator.on = true; --是否在Metastore实例上运行启动线程和清理线程
set hive.compactor.worker.threads = 1; --在此metastore实例上运行多少个压缩程序工作线程。

drop table if exists  student_trans;

CREATE TABLE student_trans (
      sno int,
      sname string,
      sdept string)
clustered by (sno) into 2 buckets stored as orc TBLPROPERTIES('transactional'='true');

--2、导入数据到student_trans中
insert overwrite table student_trans
select num,name,dept
from student;

select *
from student_trans;

--3、对student_trans建立聚合物化视图
CREATE MATERIALIZED VIEW student_trans_agg
AS SELECT sdept, count(*) as sdept_cnt from student_trans group by sdept;

--注意 这里当执行CREATE MATERIALIZED VIEW，会启动一个MR对物化视图进行构建
--可以发现当下的数据库中有了一个物化视图
show tables;
show materialized views;

--4、对原始表student_trans查询
--由于会命中物化视图，重写query查询物化视图，查询速度会加快（没有启动MR，只是普通的table scan）
SELECT sdept, count(*) as sdept_cnt from student_trans group by sdept;

--5、查询执行计划可以发现 查询被自动重写为TableScan alias: itcast.student_trans_agg
--转换成了对物化视图的查询  提高了查询效率
explain SELECT sdept, count(*) as sdept_cnt from student_trans group by sdept;


--验证禁用物化视图自动重写
ALTER MATERIALIZED VIEW student_trans_agg DISABLE REWRITE;

--删除物化视图
drop materialized view student_trans_agg;



-------------------Database 数据库 DDL操作---------------------------------------
--创建数据库
create database if not exists itcast
comment "this is my first db"
with dbproperties ('createdBy'='Allen');

--描述数据库信息
describe database itcast;
describe database extended itcast;
desc database extended itcast;

--切换数据库
use default;
use itcast;
create table t_1(id int);

--删除数据库
--注意 CASCADE关键字慎重使用
-- DROP (DATABASE|SCHEMA) [IF EXISTS] database_name [RESTRICT|CASCADE];
-- drop database itcast cascade ;


--更改数据库属性
-- ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...);
--更改数据库所有者
-- ALTER (DATABASE|SCHEMA) database_name SET OWNER [USER|ROLE] user_or_role;
--更改数据库位置
-- ALTER (DATABASE|SCHEMA) database_name SET LOCATION hdfs_path;


-------------------Table 表 DDL操作---------------------------------------

--查询指定表的元数据信息
-- describe formatted itheima.student_partition;

--1、更改表名
-- ALTER TABLE table_name RENAME TO new_table_name;
--2、更改表属性
-- ALTER TABLE table_name SET TBLPROPERTIES (property_name = property_value, ... );
--更改表注释
-- ALTER TABLE student SET TBLPROPERTIES ('comment' = "new comment for student table");
--3、更改SerDe属性
-- ALTER TABLE table_name SET SERDE serde_class_name [WITH SERDEPROPERTIES (property_name = property_value, ... )];
-- ALTER TABLE table_name [PARTITION partition_spec] SET SERDEPROPERTIES serde_properties;
-- ALTER TABLE table_name SET SERDEPROPERTIES ('field.delim' = ',');
--移除SerDe属性
-- ALTER TABLE table_name [PARTITION partition_spec] UNSET SERDEPROPERTIES (property_name, ... );

--4、更改表的文件存储格式 该操作仅更改表元数据。现有数据的任何转换都必须在Hive之外进行。
-- ALTER TABLE table_name  SET FILEFORMAT file_format;
--5、更改表的存储位置路径
-- ALTER TABLE table_name SET LOCATION "new location";

--6、更改列名称/类型/位置/注释
CREATE TABLE test_change (a int, b int, c int);
// First change column a's name to a1.
ALTER TABLE test_change CHANGE a a1 INT;
// Next change column a1's name to a2, its data type to string, and put it after column b.
ALTER TABLE test_change CHANGE a1 a2 STRING AFTER b;
// The new table's structure is:  b int, a2 string, c int.
// Then change column c's name to c1, and put it as the first column.
ALTER TABLE test_change CHANGE c c1 INT FIRST;
// The new table's structure is:  c1 int, b int, a2 string.
// Add a comment to column a1
ALTER TABLE test_change CHANGE a1 a1 INT COMMENT 'this is column a1';

--7、添加/替换列
--使用ADD COLUMNS，您可以将新列添加到现有列的末尾但在分区列之前。
--REPLACE COLUMNS 将删除所有现有列，并添加新的列集。
-- ALTER TABLE table_name ADD|REPLACE COLUMNS (col_name data_type,...);


-------------------Partition分区 DDL操作---------------------------------------
--1、增加分区
--step1: 创建表 手动加载分区数据
drop table if exists t_user_province;
create table t_user_province (
    num int,
    name string,
    sex string,
    age int,
    dept string) partitioned by (province string);

load data local inpath '/opt/data/students.txt' into table t_user_province partition(province ="SH");

--step2：添加一个分区
ALTER TABLE t_user_province ADD PARTITION (province='BJ') location
    '/user/hive/warehouse/itheima.db/t_user_province/province=BJ';

--step3:必须自己把数据加载到增加的分区中 hive不会帮你添加


----此外还支持一次添加多个分区
-- ALTER TABLE table_name ADD PARTITION (dt='2008-08-08', country='us') location '/path/to/us/part080808'
--     PARTITION (dt='2008-08-09', country='us') location '/path/to/us/part080809';


--2、重命名分区
ALTER TABLE t_user_province PARTITION (province ="SH") RENAME TO PARTITION (province ="Shanghai");

--3、删除分区
-- ALTER TABLE table_name DROP [IF EXISTS] PARTITION (dt='2008-08-08', country='us');
-- ALTER TABLE table_name DROP [IF EXISTS] PARTITION (dt='2008-08-08', country='us') PURGE; --直接删除数据 不进垃圾桶

--4、修复分区
-- MSCK [REPAIR] TABLE table_name [ADD/DROP/SYNC PARTITIONS];


--5、修改分区
--更改分区文件存储格式
-- ALTER TABLE table_name PARTITION (dt='2008-08-09') SET FILEFORMAT file_format;
--更改分区位置
-- ALTER TABLE table_name PARTITION (dt='2008-08-09') SET LOCATION "new location";


-----MSCK 修复分区---------------
--Step1：创建分区表
create table t_all_hero_part_msck(
                                     id int,
                                     name string,
                                     hp_max int,
                                     mp_max int,
                                     attack_max int,
                                     defense_max int,
                                     attack_range string,
                                     role_main string,
                                     role_assist string
) partitioned by (role string)
    row format delimited
        fields terminated by "\t";

--Step2：在linux上，使用HDFS命令创建分区文件夹
-- hadoop fs -mkdir -p /user/hive/warehouse/itheima.db/t_all_hero_part_msck/role=sheshou
-- hadoop fs -mkdir -p /user/hive/warehouse/itheima.db/t_all_hero_part_msck/role=tanke

--Step3：把数据文件上传到对应的分区文件夹下
-- hadoop fs -put archer.txt /user/hive/warehouse/itheima.db/t_all_hero_part_msck/role=sheshou
-- hadoop fs -put tank.txt /user/hive/warehouse/itheima.db/t_all_hero_part_msck/role=tanke

--Step4：查询表 可以发现没有数据
select * from t_all_hero_part_msck;
--Step5：使用MSCK命令进行修复
--add partitions可以不写 因为默认就是增加分区
MSCK repair table t_all_hero_part_msck add partitions;


--Step1：直接使用HDFS命令删除分区表的某一个分区文件夹
-- hadoop fs -rm -r /user/hive/warehouse/itheima.db/t_all_hero_part_msck/role=sheshou

--Step2：查询发现还有分区信息
--因为元数据信息没有删除
show partitions t_all_hero_part_msck;

--Step3：使用MSCK命令进行修复
MSCK repair table t_all_hero_part_msck drop partitions;





--1、显示所有数据库 SCHEMAS和DATABASES的用法 功能一样
show databases;
show schemas;

--2、显示当前数据库所有表/视图/物化视图/分区/索引
-- show tables;
-- SHOW TABLES [IN database_name]; --指定某个数据库

--3、显示当前数据库下所有视图
Show Views;
SHOW VIEWS 'test_*'; -- show all views that start with "test_"
SHOW VIEWS FROM test1; -- show views from database test1
-- SHOW VIEWS [IN/FROM database_name];

--4、显示当前数据库下所有物化视图
-- SHOW MATERIALIZED VIEWS [IN/FROM database_name];

--5、显示表分区信息，分区按字母顺序列出，不是分区表执行该语句会报错
-- show partitions table_name;
show partitions itheima.student_partition;

--6、显示表/分区的扩展信息
-- SHOW TABLE EXTENDED [IN|FROM database_name] LIKE table_name;
show table extended like student;
describe formatted itheima.student;

--7、显示表的属性信息
-- SHOW TBLPROPERTIES table_name;
show tblproperties student;

--8、显示表、视图的创建语句
-- SHOW CREATE TABLE ([db_name.]table_name|view_name);
-- show create table student;

--9、显示表中的所有列，包括分区列。
-- SHOW COLUMNS (FROM|IN) table_name [(FROM|IN) db_name];
show columns  in student;

--10、显示当前支持的所有自定义和内置的函数
show functions;

--11、Describe desc
--查看表信息
-- desc extended table_name;
--查看表信息（格式化美观）
-- desc formatted table_name;
--查看数据库相关信息
-- describe database database_name;














