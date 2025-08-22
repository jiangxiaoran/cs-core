-- 存储过程示例脚本
-- 用于测试作业分组调度系统的存储过程执行功能

-- 1. 数据清理存储过程
DELIMITER $$
CREATE PROCEDURE sp_cleanup_old_data(
    IN p_table_name VARCHAR(100),
    IN p_retention_days INT,
    IN p_cleanup_date DATETIME,
    OUT p_affected_rows INT
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    SET @sql = CONCAT('DELETE FROM ', p_table_name, ' WHERE created_time < DATE_SUB(NOW(), INTERVAL ', p_retention_days, ' DAY)');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SET p_affected_rows = ROW_COUNT();
    
    -- 记录清理日志
    INSERT INTO system_audit_log (
        operation_type, 
        table_name, 
        affected_rows, 
        operation_time, 
        executed_by
    ) VALUES (
        'CLEANUP',
        p_table_name,
        p_affected_rows,
        p_cleanup_date,
        'BATCH_SYSTEM'
    );
    
    COMMIT;
    
    SELECT CONCAT('Cleaned ', p_affected_rows, ' records from ', p_table_name) AS message;
END$$
DELIMITER ;

-- 2. 数据统计存储过程
DELIMITER $$
CREATE PROCEDURE sp_generate_daily_statistics(
    IN p_report_date DATE,
    IN p_user_id VARCHAR(50)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- 生成用户统计
    INSERT INTO daily_user_statistics (
        report_date,
        total_users,
        active_users,
        new_users,
        created_time
    )
    SELECT 
        p_report_date,
        COUNT(*) as total_users,
        COUNT(CASE WHEN last_login_date >= DATE_SUB(p_report_date, INTERVAL 7 DAY) THEN 1 END) as active_users,
        COUNT(CASE WHEN created_time >= p_report_date THEN 1 END) as new_users,
        NOW()
    FROM user_data;
    
    -- 生成订单统计
    INSERT INTO daily_order_statistics (
        report_date,
        total_orders,
        total_amount,
        avg_order_amount,
        created_time
    )
    SELECT 
        p_report_date,
        COUNT(*) as total_orders,
        SUM(order_amount) as total_amount,
        AVG(order_amount) as avg_order_amount,
        NOW()
    FROM order_data 
    WHERE DATE(created_time) = p_report_date;
    
    -- 生成产品统计
    INSERT INTO daily_product_statistics (
        report_date,
        total_products,
        active_products,
        created_time
    )
    SELECT 
        p_report_date,
        COUNT(*) as total_products,
        COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active_products,
        NOW()
    FROM product_data;
    
    COMMIT;
    
    SELECT 'Daily statistics generated successfully' AS message;
END$$
DELIMITER ;

-- 3. 数据同步存储过程
DELIMITER $$
CREATE PROCEDURE sp_sync_external_data(
    IN p_source_system VARCHAR(50),
    IN p_target_table VARCHAR(100),
    IN p_sync_timestamp TIMESTAMP
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- 根据源系统执行不同的同步逻辑
    CASE p_source_system
        WHEN 'ERP' THEN
            -- 同步ERP数据
            INSERT INTO p_target_table (
                external_id, 
                data_content, 
                sync_time, 
                source_system
            )
            SELECT 
                erp_id,
                JSON_OBJECT('name', erp_name, 'code', erp_code),
                p_sync_timestamp,
                'ERP'
            FROM external_erp_data 
            WHERE sync_status = 'PENDING';
            
        WHEN 'CRM' THEN
            -- 同步CRM数据
            INSERT INTO p_target_table (
                external_id, 
                data_content, 
                sync_time, 
                source_system
            )
            SELECT 
                crm_id,
                JSON_OBJECT('name', crm_name, 'email', crm_email),
                p_sync_timestamp,
                'CRM'
            FROM external_crm_data 
            WHERE sync_status = 'PENDING';
            
        WHEN 'OMS' THEN
            -- 同步OMS数据
            INSERT INTO p_target_table (
                external_id, 
                data_content, 
                sync_time, 
                source_system
            )
            SELECT 
                oms_id,
                JSON_OBJECT('order_no', oms_order_no, 'status', oms_status),
                p_sync_timestamp,
                'OMS'
            FROM external_oms_data 
            WHERE sync_status = 'PENDING';
            
        ELSE
            -- 通用同步逻辑
            INSERT INTO p_target_table (
                external_id, 
                data_content, 
                sync_time, 
                source_system
            )
            SELECT 
                external_id,
                data_content,
                p_sync_timestamp,
                p_source_system
            FROM external_data 
            WHERE source_system = p_source_system 
            AND sync_status = 'PENDING';
    END CASE;
    
    -- 更新同步状态
    UPDATE external_data 
    SET sync_status = 'SYNCED', 
        sync_time = p_sync_timestamp 
    WHERE source_system = p_source_system 
    AND sync_status = 'PENDING';
    
    COMMIT;
    
    SELECT CONCAT('Synced data from ', p_source_system, ' to ', p_target_table) AS message;
END$$
DELIMITER ;

-- 4. 报表生成存储过程
DELIMITER $$
CREATE PROCEDURE sp_generate_report(
    IN p_report_type VARCHAR(20),
    IN p_report_date DATE,
    IN p_generated_by VARCHAR(50)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- 根据报表类型生成不同的报表
    CASE p_report_type
        WHEN 'DAILY' THEN
            -- 生成日报
            INSERT INTO report_data (
                report_type,
                report_date,
                report_content,
                generated_by,
                created_time
            )
            SELECT 
                'DAILY',
                p_report_date,
                JSON_OBJECT(
                    'total_users', (SELECT COUNT(*) FROM user_data),
                    'total_orders', (SELECT COUNT(*) FROM order_data WHERE DATE(created_time) = p_report_date),
                    'total_revenue', (SELECT COALESCE(SUM(order_amount), 0) FROM order_data WHERE DATE(created_time) = p_report_date)
                ),
                p_generated_by,
                NOW();
                
        WHEN 'WEEKLY' THEN
            -- 生成周报
            INSERT INTO report_data (
                report_type,
                report_date,
                report_content,
                generated_by,
                created_time
            )
            SELECT 
                'WEEKLY',
                p_report_date,
                JSON_OBJECT(
                    'week_start', DATE_SUB(p_report_date, INTERVAL WEEKDAY(p_report_date) DAY),
                    'week_end', DATE_ADD(DATE_SUB(p_report_date, INTERVAL WEEKDAY(p_report_date) DAY), INTERVAL 6 DAY),
                    'total_orders', (SELECT COUNT(*) FROM order_data WHERE YEARWEEK(created_time) = YEARWEEK(p_report_date)),
                    'total_revenue', (SELECT COALESCE(SUM(order_amount), 0) FROM order_data WHERE YEARWEEK(created_time) = YEARWEEK(p_report_date))
                ),
                p_generated_by,
                NOW();
                
        WHEN 'MONTHLY' THEN
            -- 生成月报
            INSERT INTO report_data (
                report_type,
                report_date,
                report_content,
                generated_by,
                created_time
            )
            SELECT 
                'MONTHLY',
                p_report_date,
                JSON_OBJECT(
                    'month', MONTH(p_report_date),
                    'year', YEAR(p_report_date),
                    'total_orders', (SELECT COUNT(*) FROM order_data WHERE YEAR(created_time) = YEAR(p_report_date) AND MONTH(created_time) = MONTH(p_report_date)),
                    'total_revenue', (SELECT COALESCE(SUM(order_amount), 0) FROM order_data WHERE YEAR(created_time) = YEAR(p_report_date) AND MONTH(created_time) = MONTH(p_report_date))
                ),
                p_generated_by,
                NOW();
                
        ELSE
            -- 生成通用报表
            INSERT INTO report_data (
                report_type,
                report_date,
                report_content,
                generated_by,
                created_time
            )
            SELECT 
                p_report_type,
                p_report_date,
                JSON_OBJECT(
                    'report_type', p_report_type,
                    'report_date', p_report_date,
                    'total_records', (SELECT COUNT(*) FROM user_data)
                ),
                p_generated_by,
                NOW();
    END CASE;
    
    COMMIT;
    
    SELECT CONCAT('Generated ', p_report_type, ' report for ', p_report_date) AS message;
END$$
DELIMITER ;

-- 5. 数据验证存储过程
DELIMITER $$
CREATE PROCEDURE sp_validate_data_integrity(
    IN p_table_name VARCHAR(100),
    IN p_validation_rule VARCHAR(50),
    IN p_validation_date DATETIME
)
BEGIN
    DECLARE validation_count INT DEFAULT 0;
    DECLARE error_count INT DEFAULT 0;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- 根据表名和验证规则执行不同的验证
    CASE p_table_name
        WHEN 'user_data' THEN
            -- 验证用户数据完整性
            SELECT COUNT(*) INTO validation_count FROM user_data;
            SELECT COUNT(*) INTO error_count FROM user_data WHERE email IS NULL OR email = '';
            
        WHEN 'order_data' THEN
            -- 验证订单数据完整性
            SELECT COUNT(*) INTO validation_count FROM order_data;
            SELECT COUNT(*) INTO error_count FROM order_data WHERE order_amount IS NULL OR order_amount <= 0;
            
        WHEN 'product_data' THEN
            -- 验证产品数据完整性
            SELECT COUNT(*) INTO validation_count FROM product_data;
            SELECT COUNT(*) INTO error_count FROM product_data WHERE product_name IS NULL OR product_name = '';
            
        ELSE
            -- 通用验证逻辑
            SET @sql = CONCAT('SELECT COUNT(*) INTO @validation_count FROM ', p_table_name);
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
            SET validation_count = @validation_count;
            SET error_count = 0;
    END CASE;
    
    -- 记录验证结果
    INSERT INTO data_validation_log (
        table_name,
        validation_rule,
        total_records,
        error_count,
        validation_date,
        validation_status
    ) VALUES (
        p_table_name,
        p_validation_rule,
        validation_count,
        error_count,
        p_validation_date,
        CASE WHEN error_count = 0 THEN 'PASS' ELSE 'FAIL' END
    );
    
    COMMIT;
    
    SELECT CONCAT('Validated ', validation_count, ' records, found ', error_count, ' errors') AS message;
END$$
DELIMITER ;

-- 6. 数据归档存储过程
DELIMITER $$
CREATE PROCEDURE sp_archive_historical_data(
    IN p_table_name VARCHAR(100),
    IN p_archive_months INT,
    IN p_archive_date DATETIME
)
BEGIN
    DECLARE archive_count INT DEFAULT 0;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- 根据表名执行不同的归档逻辑
    CASE p_table_name
        WHEN 'system_logs' THEN
            -- 归档系统日志
            INSERT INTO system_logs_archive 
            SELECT * FROM system_logs 
            WHERE created_time < DATE_SUB(NOW(), INTERVAL p_archive_months MONTH);
            
            DELETE FROM system_logs 
            WHERE created_time < DATE_SUB(NOW(), INTERVAL p_archive_months MONTH);
            
            SET archive_count = ROW_COUNT();
            
        WHEN 'order_data' THEN
            -- 归档订单数据
            INSERT INTO order_data_archive 
            SELECT * FROM order_data 
            WHERE created_time < DATE_SUB(NOW(), INTERVAL p_archive_months MONTH);
            
            DELETE FROM order_data 
            WHERE created_time < DATE_SUB(NOW(), INTERVAL p_archive_months MONTH);
            
            SET archive_count = ROW_COUNT();
            
        WHEN 'user_data' THEN
            -- 归档用户数据（仅归档非活跃用户）
            INSERT INTO user_data_archive 
            SELECT * FROM user_data 
            WHERE last_login_date < DATE_SUB(NOW(), INTERVAL p_archive_months MONTH)
            AND status = 'INACTIVE';
            
            DELETE FROM user_data 
            WHERE last_login_date < DATE_SUB(NOW(), INTERVAL p_archive_months MONTH)
            AND status = 'INACTIVE';
            
            SET archive_count = ROW_COUNT();
            
        ELSE
            -- 通用归档逻辑
            SET @sql = CONCAT('INSERT INTO ', p_table_name, '_archive SELECT * FROM ', p_table_name, ' WHERE created_time < DATE_SUB(NOW(), INTERVAL ', p_archive_months, ' MONTH)');
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
            
            SET @sql = CONCAT('DELETE FROM ', p_table_name, ' WHERE created_time < DATE_SUB(NOW(), INTERVAL ', p_archive_months, ' MONTH)');
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
            
            SET archive_count = ROW_COUNT();
    END CASE;
    
    -- 记录归档日志
    INSERT INTO data_archive_log (
        table_name,
        archive_months,
        archived_records,
        archive_date,
        executed_by
    ) VALUES (
        p_table_name,
        p_archive_months,
        archive_count,
        p_archive_date,
        'BATCH_SYSTEM'
    );
    
    COMMIT;
    
    SELECT CONCAT('Archived ', archive_count, ' records from ', p_table_name) AS message;
END$$
DELIMITER ;

-- 7. 系统维护存储过程
DELIMITER $$
CREATE PROCEDURE sp_system_maintenance(
    IN p_maintenance_type VARCHAR(20),
    IN p_maintenance_date DATETIME,
    IN p_executed_by VARCHAR(50)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- 根据维护类型执行不同的维护操作
    CASE p_maintenance_type
        WHEN 'BACKUP' THEN
            -- 执行数据备份
            INSERT INTO system_maintenance_log (
                maintenance_type,
                maintenance_date,
                executed_by,
                maintenance_details
            ) VALUES (
                'BACKUP',
                p_maintenance_date,
                p_executed_by,
                'Database backup completed'
            );
            
        WHEN 'OPTIMIZE' THEN
            -- 执行数据库优化
            OPTIMIZE TABLE user_data, order_data, product_data, system_logs;
            
            INSERT INTO system_maintenance_log (
                maintenance_type,
                maintenance_date,
                executed_by,
                maintenance_details
            ) VALUES (
                'OPTIMIZE',
                p_maintenance_date,
                p_executed_by,
                'Database optimization completed'
            );
            
        WHEN 'CLEANUP' THEN
            -- 执行系统清理
            DELETE FROM system_logs WHERE created_time < DATE_SUB(NOW(), INTERVAL 90 DAY);
            DELETE FROM temporary_data WHERE created_time < DATE_SUB(NOW(), INTERVAL 7 DAY);
            
            INSERT INTO system_maintenance_log (
                maintenance_type,
                maintenance_date,
                executed_by,
                maintenance_details
            ) VALUES (
                'CLEANUP',
                p_maintenance_date,
                p_executed_by,
                'System cleanup completed'
            );
            
        ELSE
            -- 通用维护操作
            INSERT INTO system_maintenance_log (
                maintenance_type,
                maintenance_date,
                executed_by,
                maintenance_details
            ) VALUES (
                p_maintenance_type,
                p_maintenance_date,
                p_executed_by,
                'General maintenance completed'
            );
    END CASE;
    
    COMMIT;
    
    SELECT CONCAT('System maintenance completed: ', p_maintenance_type) AS message;
END$$
DELIMITER ;

-- 8. 用户数据同步存储过程
DELIMITER $$
CREATE PROCEDURE sp_sync_user_data(
    IN p_sync_timestamp TIMESTAMP,
    IN p_sync_type VARCHAR(20)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- 根据同步类型执行不同的同步逻辑
    CASE p_sync_type
        WHEN 'FULL' THEN
            -- 全量同步
            INSERT INTO user_data (
                username, 
                email, 
                phone, 
                status, 
                created_time, 
                updated_time
            )
            SELECT 
                external_username,
                external_email,
                external_phone,
                'ACTIVE',
                NOW(),
                NOW()
            FROM external_user_data 
            WHERE sync_status = 'PENDING'
            ON DUPLICATE KEY UPDATE
                email = VALUES(email),
                phone = VALUES(phone),
                updated_time = NOW();
                
        WHEN 'INCREMENTAL' THEN
            -- 增量同步
            INSERT INTO user_data (
                username, 
                email, 
                phone, 
                status, 
                created_time, 
                updated_time
            )
            SELECT 
                external_username,
                external_email,
                external_phone,
                'ACTIVE',
                NOW(),
                NOW()
            FROM external_user_data 
            WHERE sync_status = 'PENDING'
            AND updated_time > p_sync_timestamp
            ON DUPLICATE KEY UPDATE
                email = VALUES(email),
                phone = VALUES(phone),
                updated_time = NOW();
    END CASE;
    
    -- 更新同步状态
    UPDATE external_user_data 
    SET sync_status = 'SYNCED', 
        sync_time = p_sync_timestamp 
    WHERE sync_status = 'PENDING';
    
    COMMIT;
    
    SELECT CONCAT('User data sync completed: ', p_sync_type) AS message;
END$$
DELIMITER ;

-- 9. 订单数据处理存储过程
DELIMITER $$
CREATE PROCEDURE sp_process_order_data(
    IN p_process_date DATE,
    IN p_process_type VARCHAR(20),
    IN p_processed_by VARCHAR(50)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- 根据处理类型执行不同的处理逻辑
    CASE p_process_type
        WHEN 'DAILY' THEN
            -- 日常订单处理
            UPDATE order_data 
            SET status = 'PROCESSED',
                processed_time = NOW(),
                processed_by = p_processed_by
            WHERE DATE(created_time) = p_process_date
            AND status = 'PENDING';
            
        WHEN 'CANCELLATION' THEN
            -- 处理取消订单
            UPDATE order_data 
            SET status = 'CANCELLED',
                cancelled_time = NOW(),
                cancelled_by = p_processed_by
            WHERE status = 'PENDING'
            AND created_time < DATE_SUB(NOW(), INTERVAL 24 HOUR);
            
        WHEN 'REFUND' THEN
            -- 处理退款订单
            UPDATE order_data 
            SET status = 'REFUNDED',
                refund_time = NOW(),
                refund_by = p_processed_by
            WHERE status = 'COMPLETED'
            AND refund_requested = 1;
    END CASE;
    
    -- 记录处理日志
    INSERT INTO order_processing_log (
        process_date,
        process_type,
        processed_by,
        processed_orders,
        created_time
    )
    SELECT 
        p_process_date,
        p_process_type,
        p_processed_by,
        COUNT(*),
        NOW()
    FROM order_data 
    WHERE DATE(processed_time) = p_process_date
    AND processed_by = p_processed_by;
    
    COMMIT;
    
    SELECT CONCAT('Order processing completed: ', p_process_type) AS message;
END$$
DELIMITER ;

-- 10. 财务数据汇总存储过程
DELIMITER $$
CREATE PROCEDURE sp_generate_financial_summary(
    IN p_summary_date DATE,
    IN p_summary_type VARCHAR(20),
    IN p_generated_by VARCHAR(50)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- 根据汇总类型生成不同的财务汇总
    CASE p_summary_type
        WHEN 'REVENUE' THEN
            -- 收入汇总
            INSERT INTO financial_summary (
                summary_date,
                summary_type,
                total_amount,
                record_count,
                generated_by,
                created_time
            )
            SELECT 
                p_summary_date,
                'REVENUE',
                SUM(order_amount),
                COUNT(*),
                p_generated_by,
                NOW()
            FROM order_data 
            WHERE DATE(created_time) = p_summary_date
            AND status = 'COMPLETED';
            
        WHEN 'COST' THEN
            -- 成本汇总
            INSERT INTO financial_summary (
                summary_date,
                summary_type,
                total_amount,
                record_count,
                generated_by,
                created_time
            )
            SELECT 
                p_summary_date,
                'COST',
                SUM(cost_amount),
                COUNT(*),
                p_generated_by,
                NOW()
            FROM cost_data 
            WHERE DATE(created_time) = p_summary_date;
            
        WHEN 'PROFIT' THEN
            -- 利润汇总
            INSERT INTO financial_summary (
                summary_date,
                summary_type,
                total_amount,
                record_count,
                generated_by,
                created_time
            )
            SELECT 
                p_summary_date,
                'PROFIT',
                SUM(order_amount) - SUM(cost_amount),
                COUNT(*),
                p_generated_by,
                NOW()
            FROM order_data o
            LEFT JOIN cost_data c ON DATE(o.created_time) = DATE(c.created_time)
            WHERE DATE(o.created_time) = p_summary_date
            AND o.status = 'COMPLETED';
            
        ELSE
            -- 通用财务汇总
            INSERT INTO financial_summary (
                summary_date,
                summary_type,
                total_amount,
                record_count,
                generated_by,
                created_time
            )
            SELECT 
                p_summary_date,
                p_summary_type,
                SUM(order_amount),
                COUNT(*),
                p_generated_by,
                NOW()
            FROM order_data 
            WHERE DATE(created_time) = p_summary_date;
    END CASE;
    
    COMMIT;
    
    SELECT CONCAT('Financial summary generated: ', p_summary_type) AS message;
END$$
DELIMITER ;

-- 11. 通用批处理存储过程
DELIMITER $$
CREATE PROCEDURE sp_generic_batch_process(
    IN p_job_code VARCHAR(100),
    IN p_job_name VARCHAR(200),
    IN p_execution_time DATETIME,
    IN p_executed_by VARCHAR(50),
    IN p_process_type VARCHAR(50)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- 记录批处理执行日志
    INSERT INTO batch_process_log (
        job_code,
        job_name,
        process_type,
        execution_time,
        executed_by,
        status,
        created_time
    ) VALUES (
        p_job_code,
        p_job_name,
        p_process_type,
        p_execution_time,
        p_executed_by,
        'SUCCESS',
        NOW()
    );
    
    -- 根据处理类型执行不同的逻辑
    CASE p_process_type
        WHEN 'DATA_PROCESSING' THEN
            -- 数据处理逻辑
            UPDATE system_status SET last_data_process = NOW() WHERE id = 1;
            
        WHEN 'SYSTEM_CHECK' THEN
            -- 系统检查逻辑
            UPDATE system_status SET last_system_check = NOW() WHERE id = 1;
            
        WHEN 'LOG_ROTATION' THEN
            -- 日志轮转逻辑
            DELETE FROM system_logs WHERE created_time < DATE_SUB(NOW(), INTERVAL 30 DAY);
            
        ELSE
            -- 通用处理逻辑
            UPDATE system_status SET last_generic_process = NOW() WHERE id = 1;
    END CASE;
    
    COMMIT;
    
    SELECT CONCAT('Generic batch process completed: ', p_process_type) AS message;
END$$
DELIMITER ;

-- 创建必要的辅助表（如果不存在）
CREATE TABLE IF NOT EXISTS system_audit_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    operation_type VARCHAR(50),
    table_name VARCHAR(100),
    affected_rows INT,
    operation_time DATETIME,
    executed_by VARCHAR(50),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_user_statistics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    report_date DATE,
    total_users INT,
    active_users INT,
    new_users INT,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_order_statistics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    report_date DATE,
    total_orders INT,
    total_amount DECIMAL(15,2),
    avg_order_amount DECIMAL(15,2),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_product_statistics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    report_date DATE,
    total_products INT,
    active_products INT,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS report_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    report_type VARCHAR(20),
    report_date DATE,
    report_content JSON,
    generated_by VARCHAR(50),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_validation_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(100),
    validation_rule VARCHAR(50),
    total_records INT,
    error_count INT,
    validation_date DATETIME,
    validation_status VARCHAR(20),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_archive_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(100),
    archive_months INT,
    archived_records INT,
    archive_date DATETIME,
    executed_by VARCHAR(50),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS system_maintenance_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    maintenance_type VARCHAR(20),
    maintenance_date DATETIME,
    executed_by VARCHAR(50),
    maintenance_details TEXT,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_processing_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    process_date DATE,
    process_type VARCHAR(20),
    processed_by VARCHAR(50),
    processed_orders INT,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS financial_summary (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    summary_date DATE,
    summary_type VARCHAR(20),
    total_amount DECIMAL(15,2),
    record_count INT,
    generated_by VARCHAR(50),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS batch_process_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_code VARCHAR(100),
    job_name VARCHAR(200),
    process_type VARCHAR(50),
    execution_time DATETIME,
    executed_by VARCHAR(50),
    status VARCHAR(20),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS system_status (
    id INT PRIMARY KEY DEFAULT 1,
    last_data_process DATETIME,
    last_system_check DATETIME,
    last_generic_process DATETIME,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 插入初始系统状态记录
INSERT IGNORE INTO system_status (id) VALUES (1); 