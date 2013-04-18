create table if not exists mysql.qqueue_usrGrps(
    id int not null auto_increment,
    name char(64) not null,
    priority int not null,
    primary key (id),
    key id_name (name)
) engine=MyISAM default charset=utf8 collate=utf8_bin;
create table if not exists mysql.qqueue_queues(
    id int not null auto_increment,
    name char(64) not null,
    priority int not null,
    timeout int not null,
    primary key (id),
    key id_name (name)
) engine=MyISAM default charset=utf8 collate=utf8_bin;
create table if not exists mysql.qqueue_jobs(
    id bigint not null,
    mysqlUserName char(64) not null,
    usrId int not null,
    usrGroup int not null,
    queue int not null,
    priority int not null,
    query text not null,
    status smallint not null default 1,
    resultDBName char(128) not null,
    resultTableName char(128) not null,
    paquFlag bool not null default 0,
    timeSubmit datetime,
    timeExecute datetime,
    timeFinish datetime,
    actualQuery text,
    error char(255) default null,
    comment text,
    primary key (id),
    key id_priority (status asc, priority desc, timeSubmit asc)
) engine=InnoDB default charset=utf8 collate=utf8_bin;
create table if not exists mysql.qqueue_history(
    id bigint not null,
    mysqlUserName char(64) not null,
    usrId int not null,
    usrGroup int not null,
    queue int not null,
    priority int not null,
    query text not null,
    status smallint not null default 1,
    resultDBName char(128) not null,
    resultTableName char(128) not null,
    paquFlag bool not null default 0,
    timeSubmit datetime,
    timeExecute datetime,
    timeFinish datetime,
    actualQuery text,
    error char(255) default null,
    comment text,
    primary key (id),
    key id_priority (status asc, priority desc, timeSubmit asc)
) engine=InnoDB default charset=utf8 collate=utf8_bin;

INSTALL PLUGIN qqueue SONAME 'daemon_jobqueue.so';
CREATE FUNCTION qqueue_addUsrGrp RETURNS INTEGER SONAME 'daemon_jobqueue.so';
CREATE FUNCTION qqueue_updateUsrGrp RETURNS INTEGER SONAME 'daemon_jobqueue.so';
CREATE FUNCTION qqueue_flushUsrGrps RETURNS INTEGER SONAME 'daemon_jobqueue.so';
CREATE FUNCTION qqueue_addQueue RETURNS INTEGER SONAME 'daemon_jobqueue.so';
CREATE FUNCTION qqueue_updateQueue RETURNS INTEGER SONAME 'daemon_jobqueue.so';
CREATE FUNCTION qqueue_flushQueues RETURNS INTEGER SONAME 'daemon_jobqueue.so';
CREATE FUNCTION qqueue_addJob RETURNS INTEGER SONAME 'daemon_jobqueue.so';
CREATE FUNCTION qqueue_killJob RETURNS INTEGER SONAME 'daemon_jobqueue.so';


-- CREATE FUNCTION qqueue_execJob RETURNS INTEGER SONAME 'daemon_jobqueue.so';
