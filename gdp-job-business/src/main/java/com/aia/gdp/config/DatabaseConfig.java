package com.aia.gdp.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

/**
 * 数据库检测配置类
 * 检测数据库连接 表是否完整
 * 集成自定义数据源配置
 *
 * @author andy
 * @date 2025-08-02
 * @company
 */
@Configuration
public class DatabaseConfig {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfig.class);
    
    @Autowired
    private DataSource dataSource;
    
    @Autowired
    private CustomDataSourceConfig customDataSourceConfig;
    
    @Bean
    public CommandLineRunner databaseCheckRunner() {
        return args -> {
            logger.info("=== 数据库连接检查开始 ===");
            
            // 记录数据源配置信息
            customDataSourceConfig.logDataSourceInfo();
            
            try (Connection connection = dataSource.getConnection()) {
                // 检查连接
                logger.info("✓ 数据库连接成功");
                
                // 获取数据库信息
                DatabaseMetaData metaData = connection.getMetaData();
                logger.info("数据库产品: {}", metaData.getDatabaseProductName());
                logger.info("数据库版本: {}", metaData.getDatabaseProductVersion());
                logger.info("驱动名称: {}", metaData.getDriverName());
                logger.info("驱动版本: {}", metaData.getDriverVersion());
                
                // 检查数据库名称
                String databaseName = connection.getCatalog();
                logger.info("当前数据库: {}", databaseName);
                
                // 检查必要的表是否存在
                checkRequiredTables(connection);
                
                logger.info("=== 数据库连接检查完成 ===");
                
            } catch (Exception e) {
                logger.error("✗ 数据库连接失败: {}", e.getMessage());
                logger.error("请检查以下配置:");
                logger.error("1. MySQL服务是否启动");
                logger.error("2. 数据库用户名和密码是否正确");
                logger.error("3. 数据库是否存在");
                logger.error("4. 网络连接是否正常");
                logger.error("5. 加密密钥是否正确设置");
                throw e;
            }
        };
    }
    
    private void checkRequiredTables(Connection connection) throws Exception {
        String[] requiredTables = {"job_def", "job_execution_log", "email_notification", "users", "sys_oper_log"};
        
        for (String tableName : requiredTables) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, tableName, null)) {
                if (rs.next()) {
                    logger.info("✓ 表 {} 存在", tableName);
                    
                    // 检查表中的数据
                    try (ResultSet countRs = connection.createStatement()
                            .executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                        if (countRs.next()) {
                            int count = countRs.getInt(1);
                            logger.info("  表 {} 中有 {} 条记录", tableName, count);
                        }
                    }
                } else {
                    logger.warn("✗ 表 {} 不存在", tableName);
                }
            }
        }
    }
} 