-- getting rid of all the data tables (you can skip this if you just want to reinstall)
drop table mysql.qqueue_usrGrps;
drop table mysql.qqueue_queues;
drop table mysql.qqueue_jobs;
drop table mysql.qqueue_history;

-- uninstalling all the UDF functions
DROP FUNCTION qqueue_addUsrGrp;
DROP FUNCTION qqueue_updateUsrGrp;
DROP FUNCTION qqueue_flushUsrGrps;
DROP FUNCTION qqueue_addQueue;
DROP FUNCTION qqueue_updateQueue;
DROP FUNCTION qqueue_flushQueues;
DROP FUNCTION qqueue_addJob;
DROP FUNCTION qqueue_killJob;

-- stop the daemon and uninstall
UNINSTALL PLUGIN qqueue;

