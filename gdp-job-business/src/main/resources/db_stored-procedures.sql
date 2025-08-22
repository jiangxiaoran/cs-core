-- 设置分隔符为 $$ 以便创建存储过程
DELIMITER $$

DROP PROCEDURE IF EXISTS `fr_pricing_ASOReportBL`$$

CREATE PROCEDURE `fr_pricing_ASOReportBL`(
    IN in_census_date VARCHAR(8)
)
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('fr_pricing_ASOReportBL GROUP_A',in_census_date,'1',now());
DO SLEEP(30);

    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$


-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `fr_pricing_ActIBNRReportBL`$$

CREATE PROCEDURE `fr_pricing_ActIBNRReportBL`(
    IN in_deal_date VARCHAR(8)
)
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('fr_pricing_ActIBNRReportBL GROUP_A',in_deal_date,'1',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$


-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `fr_pricing_Batch_GTA_REPORT_TASK`$$

CREATE PROCEDURE `fr_pricing_Batch_GTA_REPORT_TASK`(
    IN in_start_date VARCHAR(10),
    IN in_end_date   VARCHAR(10)
)
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('fr_pricing_Batch_GTA_REPORT_TASK GROUP_A',in_start_date,'14',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$


-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `fr_pricing_CASUPRAnnualizedPremiumReportBL`$$

CREATE PROCEDURE `fr_pricing_CASUPRAnnualizedPremiumReportBL`(
    IN in_deal_date VARCHAR(10)
)
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('fr_pricing_CASUPRAnnualizedPremiumReportBL GROUP_A',in_deal_date,'1',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$


-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `fr_pricing_GVSCnolmmReportBL`$$

CREATE PROCEDURE `fr_pricing_GVSCnolmmReportBL`(IN pCensusDate VARCHAR(20))
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('fr_pricing_GVSCnolmmReportBL GROUP_B',pCensusDate,'5',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$


-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `fr_pricing_GVSCnolmmReportDateBL`$$

CREATE PROCEDURE `fr_pricing_GVSCnolmmReportDateBL`(
    IN in_census_date VARCHAR(10)
)
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('fr_pricing_GVSCnolmmReportDateBL GROUP_B',in_census_date,'15',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$

-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `fr_pricing_GVS_CNOLMM_REPORT`$$

CREATE PROCEDURE `fr_pricing_GVS_CNOLMM_REPORT`(IN pCensusDate  VARCHAR(10))
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('fr_pricing_GVS_CNOLMM_REPORT GROUP_B',pCensusDate,'6',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$


-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `fr_pricing_IFRSCoolOffGrpContDataCreateBL`$$

CREATE PROCEDURE `fr_pricing_IFRSCoolOffGrpContDataCreateBL`(
    IN in_start_date VARCHAR(10),
    IN in_end_date VARCHAR(10)
)
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('fr_pricing_IFRSCoolOffGrpContDataCreateBL GROUP_B',in_start_date,'7',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$


-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `fr_pricing_IFRS_EB_01`$$

CREATE PROCEDURE `fr_pricing_IFRS_EB_01`(
    IN in_deal_date VARCHAR(8),
    IN in_gvsmvn_start VARCHAR(8)
)
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('fr_pricing_IFRSCoolOffGrpContDataCreateBL',in_deal_date,'8',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$


-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `fr_pricing_IFRS_GVS_07`$$

CREATE PROCEDURE `fr_pricing_IFRS_GVS_07`(
    IN in_deal_date VARCHAR(8),
    IN in_gvsmvn_start VARCHAR(8)
)
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('fr_pricing_IFRS_GVS_07',in_deal_date ,'9',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$


-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `fr_pricing_UPRMonthDataReportBL`$$

CREATE PROCEDURE `fr_pricing_UPRMonthDataReportBL`(
    IN in_deal_date VARCHAR(10)
)
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('fr_pricing_UPRMonthDataReportBL',in_deal_date,'10',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$


-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `pfr_pricing_GVSCompassnbReportTwoBL`$$

CREATE PROCEDURE `pfr_pricing_GVSCompassnbReportTwoBL`(
    IN in_census_date VARCHAR(8)
)
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('pfr_pricing_GVSCompassnbReportTwoBL',in_census_date,'11',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$


-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `pfr_pricing_IFRS_LIC_01`$$

CREATE PROCEDURE `pfr_pricing_IFRS_LIC_01`(
    IN in_deal_date VARCHAR(8)
)
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('pfr_pricing_IFRS_LIC_01',in_deal_date,'12',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$


-- 删除已存在的存储过程（如果存在）
DROP PROCEDURE IF EXISTS `pfr_pricing_IFRS_UDP_POL_INFORCE`$$

CREATE PROCEDURE `pfr_pricing_IFRS_UDP_POL_INFORCE`(
    IN in_deal_date VARCHAR(8)
)
BEGIN
    -- 异常处理
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
END;

START TRANSACTION;
insert into test values('pfr_pricing_IFRS_UDP_POL_INFORCE',in_deal_date,'13',now());
DO SLEEP(30);
    -- 示例业务逻辑（你可以替换为实际逻辑）
    -- SELECT in_start_date, in_end_date;

COMMIT;
END$$




-- 恢复默认分隔符
DELIMITER ;