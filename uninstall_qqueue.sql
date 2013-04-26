-- getting rid of all the data tables (you can skip this if you just want to reinstall)
DROP TABLE mysql.qqueue_usrGrps;
DROP TABLE mysql.qqueue_queues;
DROP TABLE mysql.qqueue_jobs;
DROP TABLE mysql.qqueue_history;

-- uninstalling all the UDF functions
DROP FUNCTION qqueue_addUsrGrp;
DROP FUNCTION qqueue_updateUsrGrp;
DROP FUNCTION qqueue_flushUsrGrps;
DROP FUNCTION qqueue_addQueue;
DROP FUNCTION qqueue_updateQueue;
DROP FUNCTION qqueue_flushQueues;
DROP FUNCTION qqueue_addJob;
DROP FUNCTION qqueue_killJob;

-- uninstalling all the procedures
USE mysql;
DROP PROCEDURE IF EXISTS qqueue_clean_history;
DROP PROCEDURE IF EXISTS qqueue_wipe_history;

-- stop the daemon and uninstall
UNINSTALL PLUGIN qqueue;

