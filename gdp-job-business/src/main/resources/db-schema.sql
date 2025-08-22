-- 数据库表结构创建脚本
-- 用于 gdp-job-business 项目

-- 创建作业定义表
DROP TABLE IF EXISTS job_def;
CREATE TABLE IF NOT EXISTS job_def (
    job_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_code VARCHAR(100) NOT NULL UNIQUE COMMENT '作业代码',
    job_name VARCHAR(200) NOT NULL COMMENT '作业名称',
    job_group VARCHAR(50) COMMENT '作业分组',
    job_order INT DEFAULT 0 COMMENT '作业顺序',
    group_order INT DEFAULT 0 COMMENT '分组顺序',
    proc_name VARCHAR(200) COMMENT '存储过程名称',
    job_params VARCHAR(500) COMMENT '作业参数',
    job_type VARCHAR(50) COMMENT '作业类型',
    schedule_time TIME COMMENT '调度时间',
    status VARCHAR(20) DEFAULT 'ACTIVE' COMMENT '状态',
    timeout_sec INT DEFAULT 3600 COMMENT '超时时间(秒)',
    retry_count INT DEFAULT 0 COMMENT '重试次数',
    notify_email VARCHAR(200) COMMENT '通知邮箱',
    is_depend BOOLEAN DEFAULT FALSE COMMENT '是否有依赖',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
    last_run_time DATETIME COMMENT '最后运行时间',
    next_run_time DATETIME COMMENT '下次运行时间',
    create_time DATETIME DEFAULT NULL COMMENT '创建时间',
    update_time DATETIME DEFAULT NULL  COMMENT '更新时间',
    INDEX idx_job_group (job_group),
    INDEX idx_job_code (job_code),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='作业定义表';

-- 创建作业执行日志表
DROP TABLE IF EXISTS job_execution_log;
CREATE TABLE IF NOT EXISTS job_execution_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_code VARCHAR(100) NOT NULL COMMENT '作业代码',
    batch_no VARCHAR(100) COMMENT '批次号',
    executor_proc VARCHAR(200) COMMENT '执行器进程',
    executor_address VARCHAR(200) COMMENT '执行器地址',
    start_time DATETIME NOT NULL COMMENT '开始时间',
    end_time DATETIME COMMENT '结束时间',
    status VARCHAR(20) NOT NULL COMMENT '执行状态',
    error_message TEXT COMMENT '错误信息',
    duration INT COMMENT '执行时长(秒)',
    notify_status VARCHAR(20) DEFAULT 'PENDING' COMMENT '通知状态',
    retry_count INT DEFAULT 0 COMMENT '重试次数',
    create_time DATETIME DEFAULT NULL COMMENT '创建时间',
    INDEX idx_job_code (job_code),
    INDEX idx_start_time (start_time),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='作业执行日志表';

-- 创建邮件通知表
DROP TABLE IF EXISTS email_notification;
CREATE TABLE IF NOT EXISTS email_notification (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_code VARCHAR(100) COMMENT '作业代码',
    recipient_email VARCHAR(200) NOT NULL COMMENT '收件人邮箱',
    subject VARCHAR(500) COMMENT '邮件主题',
    body TEXT COMMENT '邮件内容',
    status VARCHAR(20) DEFAULT 'PENDING' COMMENT '发送状态',
    send_time DATETIME COMMENT '发送时间',
    created_at DATETIME DEFAULT NULL COMMENT '创建时间',
    updated_at DATETIME DEFAULT NULL COMMENT '更新时间',
    fail_reason TEXT COMMENT '失败原因',
    type VARCHAR(50) COMMENT '通知类型',
    retry_count INT DEFAULT 0 COMMENT '重试次数',
    is_system BOOLEAN DEFAULT FALSE COMMENT '是否系统通知',
    INDEX idx_job_code (job_code),
    INDEX idx_status (status),
    INDEX idx_send_time (send_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='邮件通知表';

DROP TABLE IF EXISTS users;
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '用户ID',
    username VARCHAR(50) NOT NULL UNIQUE COMMENT '用户名',
    password VARCHAR(255) NOT NULL COMMENT '密码',
    real_name VARCHAR(100) COMMENT '真实姓名',
    email VARCHAR(100) COMMENT '邮箱',
    phone VARCHAR(20) COMMENT '手机号',
    avatar VARCHAR(255) COMMENT '头像URL',
    role VARCHAR(50) DEFAULT 'user' COMMENT '角色',
    permissions TEXT COMMENT '权限列表（逗号分隔）',
    last_login_time DATETIME COMMENT '最后登录时间',
    create_time DATETIME DEFAULT NULL COMMENT '创建时间',
    update_time DATETIME DEFAULT NULL COMMENT '更新时间',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
    
    INDEX idx_username (username),
    INDEX idx_email (email),
    INDEX idx_role (role),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';

DROP TABLE IF EXISTS sys_oper_log;
CREATE TABLE sys_oper_log (
            oper_id         BIGINT AUTO_INCREMENT PRIMARY KEY,  -- 日志主键（自增）
            title           VARCHAR(50)  DEFAULT '',          -- 模块标题
            business_type   SMALLINT     DEFAULT 0,           -- 业务类型（0其它 1新增 2修改 3删除）
            method          VARCHAR(200) DEFAULT '',          -- 方法名称
            request_method  VARCHAR(10)  DEFAULT '',          -- 请求方式
            operator_type   SMALLINT     DEFAULT 0,           -- 操作类别（0其它 1后台用户 2手机端用户）
            oper_name       VARCHAR(50)  DEFAULT '',          -- 操作人员
            dept_name       VARCHAR(50)  DEFAULT '',          -- 部门名称
            oper_url        VARCHAR(255) DEFAULT '',          -- 请求URL
            oper_ip         VARCHAR(128) DEFAULT '',          -- 主机地址
            oper_location   VARCHAR(255) DEFAULT '',          -- 操作地点
            oper_param      VARCHAR(2000) DEFAULT '',         -- 请求参数
            json_result     VARCHAR(2000) DEFAULT '',         -- 返回参数
            status          SMALLINT     DEFAULT 0,           -- 操作状态（0正常 1异常）
            error_msg       VARCHAR(2000) DEFAULT '',         -- 错误消息
            oper_time       DATETIME,                        -- 操作时间
            cost_time       BIGINT       DEFAULT 0            -- 消耗时间
);
--测试存储过程调用是否成功 调用成功插入数据
DROP TABLE IF EXISTS test;

CREATE TABLE test (
  name varchar(100) DEFAULT '', 
  value varchar(100) DEFAULT '', 
  `order` int DEFAULT 0,
  createdat DATETIME
);

INSERT INTO users
(user_id, username, password, real_name, email, phone, avatar, role, permissions, last_login_time, create_time, update_time, is_active)
VALUES(1, 'admin', '$2a$10$UHA95MgCJKWizAO9krFcqO43kx3ExBxBp4B0XbFNgkvaLrrIxAuve', '管理员', 'admin@example.com', NULL, NULL, 'admin', 'job:read,job:write,job:delete,job:control,log:read,log:write,log:delete,system:read,system:write', '2025-08-18 09:36:38', '2025-08-07 16:08:09', '2025-08-07 17:27:16', 1),
(2, 'user', '$2a$10$UHA95MgCJKWizAO9krFcqO43kx3ExBxBp4B0XbFNgkvaLrrIxAuve', '普通用户', 'user@example.com', NULL, NULL, 'user', 'job:read,log:read', '2025-08-12 14:33:38', '2025-08-07 16:08:09', '2025-08-07 17:27:16', 1);


--INSERT INTO xxl_job.xxl_job_info
--(id, job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status, trigger_last_time, trigger_next_time)
--VALUES(1, 1, 'gdb 精算', '2025-07-14 18:23:24', '2025-07-25 14:49:40', 'XXL', '', 'CRON', '0 0 0 * * ? *', 'DO_NOTHING', 'FIRST', 'jobGroupDispatcher', 'job01,job02', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', '2025-07-14 18:23:24', '', 0, 0, 0);

--INSERT INTO xxl_job.xxl_job_registry
--(id, registry_group, registry_key, registry_value, update_time)
--VALUES(17, 'EXECUTOR', 'gdp-job-business', 'http://172.26.150.132:9999/', '2025-08-18 14:48:52');

--存储过程正式数据

--fr_pricing_ASOReportBL  {"in_census_date":"${today}"} in_census_date VARCHAR(8)
--fr_pricing_ActIBNRReportBL   {"in_deal_date":"${today}"}  in_deal_date VARCHAR(8)
--fr_pricing_Batch_GTA_REPORT_TASK   {"in_start_date":"${date-1d}","in_end_date":"${date+1d}}"}   IN in_start_date VARCHAR(10),
--   IN in_end_date   VARCHAR(10)
--fr_pricing_CASUPRAnnualizedPremiumReportBL {"in_deal_date":"${today}"}   in_deal_date VARCHAR(10)
--fr_pricing_GVSCnolmmReportBL   {"pCensusDate":"${today}"}  pCensusDate VARCHAR(20)
--fr_pricing_GVSCnolmmReportDateBL   {"in_census_date":"${today}"}  in_census_date VARCHAR(10)
--fr_pricing_GVS_CNOLMM_REPORT  {"pCensusDate":"${today}"}  pCensusDate  VARCHAR(10)
--fr_pricing_IFRSCoolOffGrpContDataCreateBL {"in_start_date":"${date-1d}","in_end_date":"${date+1d}}"}    IN in_start_date VARCHAR(8),
--   IN in_end_date VARCHAR(8)
--fr_pricing_IFRS_EB_01   {"in_deal_date":"${date-1d}","in_gvsmvn_start":"${date+1d}}"}     IN in_deal_date VARCHAR(8),
--   IN in_gvsmvn_start VARCHAR(8)
--fr_pricing_IFRS_GVS_07    {"in_deal_date":"${date-1d}","in_gvsmvn_start":"${date+1d}}"}     IN in_deal_date VARCHAR(8),
--   IN in_gvsmvn_start VARCHAR(8)
--fr_pricing_UPRMonthDataReportBL {"in_deal_date":"${today}"}     IN in_deal_date VARCHAR(10)
--pfr_pricing_GVSCompassnbReportTwoBL   {"in_census_date":"${today}"}     IN in_census_date VARCHAR(8)
--pfr_pricing_IFRS_LIC_01    {"in_deal_date":"${today}"}   IN in_deal_date VARCHAR(8)
--pfr_pricing_IFRS_UDP_POL_INFORCE {"in_deal_date":"${today}"}  IN in_deal_date VARCHAR(8)





INSERT INTO job_def 
(job_code, job_name, proc_name, job_params, job_type, job_group, job_order, group_order, is_active) VALUES
('fr_pricing_ASOReportBL', 'fr_pricing_ASOReportBL', 'fr_pricing_ASOReportBL', '{"in_census_date":"\${today}"}', 'stored_procedure', 'GROUP_A', 1, 1, 1),
('fr_pricing_ActIBNRReportBL', 'fr_pricing_ActIBNRReportBL', 'fr_pricing_ActIBNRReportBL', '{"in_deal_date":"\${today}"}', 'stored_procedure', 'GROUP_A', 2, 1, 1),
('fr_pricing_Batch_GTA_REPORT_TASK', 'fr_pricing_Batch_GTA_REPORT_TASK', 'fr_pricing_Batch_GTA_REPORT_TASK', '{"in_start_date":"${date-1d}","in_end_date":"${date+1d}"}', 'stored_procedure', 'GROUP_A', 3, 1, 1),
('fr_pricing_CASUPRAnnualizedPremiumReportBL', 'fr_pricing_CASUPRAnnualizedPremiumReportBL', 'fr_pricing_CASUPRAnnualizedPremiumReportBL', '{"in_deal_date":"\${today}"}', 'stored_procedure', 'GROUP_A', 4, 1, 1),
('fr_pricing_GVSCnolmmReportBL', 'fr_pricing_GVSCnolmmReportBL', 'fr_pricing_GVSCnolmmReportBL', '{"pCensusDate":"\${today}"}', 'stored_procedure', 'GROUP_B', 1, 2, 1),
('fr_pricing_GVSCnolmmReportDateBL', 'fr_pricing_GVSCnolmmReportDateBL', 'fr_pricing_GVSCnolmmReportDateBL', '{"in_census_date":"\${today}"}', 'stored_procedure', 'GROUP_B', 2, 2, 1),
('fr_pricing_GVS_CNOLMM_REPORT', 'fr_pricing_GVS_CNOLMM_REPORT', 'fr_pricing_GVS_CNOLMM_REPORT', '{"pCensusDate":"\${today}"}', 'stored_procedure', 'GROUP_B', 3, 2, 1),
('fr_pricing_IFRSCoolOffGrpContDataCreateBL', 'fr_pricing_IFRSCoolOffGrpContDataCreateBL', 'fr_pricing_IFRSCoolOffGrpContDataCreateBL', '{"in_start_date":"${date-1d}","in_end_date":"${date+1d}"}', 'stored_procedure', 'GROUP_B', 4, 2, 1),
('fr_pricing_IFRS_EB_01', 'fr_pricing_IFRS_EB_01', 'fr_pricing_IFRS_EB_01', '{"in_deal_date":"${date-1d}","in_gvsmvn_start":"${date+1d}"}', 'stored_procedure', 'GROUP_C', 1, 3, 1),
('fr_pricing_IFRS_GVS_07', 'fr_pricing_IFRS_GVS_07', 'fr_pricing_IFRS_GVS_07', '{"in_deal_date":"${date-1d}","in_gvsmvn_start":"${date+1d}"}', 'stored_procedure', 'GROUP_C', 2, 3, 1),
('fr_pricing_UPRMonthDataReportBL', 'fr_pricing_UPRMonthDataReportBL', 'fr_pricing_UPRMonthDataReportBL', '{"in_deal_date":"\${today}"}', 'stored_procedure', 'GROUP_C', 3, 3, 1),
('fr_pricing_GVSCompassnbReportTwoBL', 'fr_pricing_GVSCompassnbReportTwoBL', 'fr_pricing_GVSCompassnbReportTwoBL', '{"in_census_date":"\${today}"}', 'stored_procedure', 'GROUP_C', 4, 3, 1),
('fr_pricing_IFRS_LIC_01', 'fr_pricing_IFRS_LIC_01', 'fr_pricing_IFRS_LIC_01', '{"in_deal_date":"\${today}"}', 'stored_procedure', 'GROUP_E', 1, 5, 1),
('fr_pricing_IFRS_UDP_POL_INFORCE', 'fr_pricing_IFRS_UDP_POL_INFORCE', 'fr_pricing_IFRS_UDP_POL_INFORCE', '{"in_deal_date":"\${today}"}', 'stored_procedure', 'GROUP_E', 2, 5, 1);




--依赖全的测试
--INSERT INTO job_def (job_code, job_name, job_type, job_group, job_order, group_order, is_active) VALUES
--('TEST001', '用户数据导入', 'data_import', 'GROUP_A', 1, 1, true),
--('TEST002', '订单数据导入', 'data_import', 'GROUP_A', 2, 1, true),
--('TEST003', '库存数据导入', 'data_import', 'GROUP_A', 3, 1, true),
--('TEST_FAIL001', '测试失败作业1', 'data_import', 'GROUP_A', 4, 1, true),
--('TEST004', '销售报表生成', 'report_generation', 'GROUP_B', 1, 2, true),
--('TEST005', '库存月报生成', 'report_generation', 'GROUP_B', 2, 2, true),
--('TEST006', '财务月报生成', 'report_generation', 'GROUP_B', 3, 2, true),
--('TEST_FAIL002', '测试失败作业2', 'report_generation', 'GROUP_B', 4, 2, true),
--('TEST007', '数据清理作业', 'data_cleanup', 'GROUP_C', 1, 3, true),
--('TEST008', '日志清理作业', 'data_cleanup', 'GROUP_C', 2, 3, true),
--('TEST009', '临时文件清理', 'data_cleanup', 'GROUP_C', 3, 3, true),
--('TEST010', '系统通知作业', 'notification', 'GROUP_C', 4, 3, true),
--('SP001', '数据清理存储过程', 'stored_procedure', 'GROUP_E', 1, 5, true),
--('SP002', '报表生成存储过程', 'stored_procedure', 'GROUP_E', 2, 5, true),
--('SP003', '统计汇总存储过程', 'stored_procedure', 'GROUP_E', 3, 5, true),
--('SP004', '数据同步存储过程', 'stored_procedure', 'GROUP_E', 4, 5, true);

