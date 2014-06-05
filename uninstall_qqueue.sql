-- getting rid of all the data tables (you can skip this if you just want to reinstall)
DROP TABLE IF EXISTS mysql.qqueue_usrGrps;
DROP TABLE IF EXISTS mysql.qqueue_queues;
DROP TABLE IF EXISTS mysql.qqueue_jobs;
DROP TABLE IF EXISTS mysql.qqueue_history;

-- uninstalling all the UDF functions
DROP FUNCTION IF EXISTS qqueue_addUsrGrp;
DROP FUNCTION IF EXISTS qqueue_updateUsrGrp;
DROP FUNCTION IF EXISTS qqueue_flushUsrGrps;
DROP FUNCTION IF EXISTS qqueue_addQueue;
DROP FUNCTION IF EXISTS qqueue_updateQueue;
DROP FUNCTION IF EXISTS qqueue_flushQueues;
DROP FUNCTION IF EXISTS qqueue_addJob;
DROP FUNCTION IF EXISTS qqueue_killJob;

-- uninstalling all the procedures
USE mysql;
DROP PROCEDURE IF EXISTS qqueue_clean_history;
DROP PROCEDURE IF EXISTS qqueue_wipe_history;

-- stop the daemon and uninstall
USE mysql;
DELIMITER //
DROP PROCEDURE IF EXISTS UninstallPlugin;
CREATE PROCEDURE UninstallPlugin()
BEGIN
IF (SELECT 1 = 1 FROM mysql.plugin WHERE `name`='qqueue') THEN UNINSTALL PLUGIN qqueue;
END IF;
END //
DELIMITER ;
CALL UninstallPlugin();
DROP PROCEDURE UninstallPlugin;

