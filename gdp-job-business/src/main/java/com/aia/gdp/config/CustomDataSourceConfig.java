package com.aia.gdp.config;

import com.aia.gdp.common.AesDecryptor;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

import javax.sql.DataSource;

/**
 * 自定义数据源配置类
 * 支持加密密码解密，完全控制数据源配置
 *
 * @author andy
 * @date 2025-01-XX
 */
@Configuration
public class CustomDataSourceConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(CustomDataSourceConfig.class);
    
    @Value("${spring.datasource.url}")
    private String url;
    
    @Value("${spring.datasource.username}")
    private String username;
    
    @Value("${spring.datasource.password}")
    private String password;
    
    @Value("${spring.datasource.driver-class-name}")
    private String driverClassName;
    
    @Value("${spring.datasource.hikari.maximum-pool-size:20}")
    private int maximumPoolSize;
    
    @Value("${spring.datasource.hikari.minimum-idle:5}")
    private int minimumIdle;
    
    @Value("${spring.datasource.hikari.connection-timeout:30000}")
    private long connectionTimeout;
    
    @Value("${spring.datasource.hikari.idle-timeout:600000}")
    private long idleTimeout;
    
    @Value("${spring.datasource.hikari.max-lifetime:1800000}")
    private long maxLifetime;
    
    @Value("${spring.datasource.hikari.leak-detection-threshold:60000}")
    private long leakDetectionThreshold;
    
    @Value("${spring.datasource.hikari.validation-timeout:5000}")
    private long validationTimeout;
    
    /**
     * 创建自定义数据源
     * 支持加密密码解密
     */
    @Bean
    @Primary
    public DataSource dataSource() {
        logger.info("Creating custom datasource with encrypted password support");
        
        // 解密密码
        String decryptedPassword = decryptPassword(password);
        
        // 创建 HikariCP 配置
        HikariConfig config = new HikariConfig();
        
        // 基本连接配置
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(decryptedPassword);
        config.setDriverClassName(driverClassName);
        
        // 连接池配置
        config.setMaximumPoolSize(maximumPoolSize);
        config.setMinimumIdle(minimumIdle);
        config.setConnectionTimeout(connectionTimeout);
        config.setIdleTimeout(idleTimeout);
        config.setMaxLifetime(maxLifetime);
        config.setLeakDetectionThreshold(leakDetectionThreshold);
        config.setValidationTimeout(validationTimeout);
        
        // 连接测试配置
        config.setConnectionTestQuery("SELECT 1");
        config.setValidationTimeout(5000);
        
        // MySQL 性能优化配置
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("useLocalSessionState", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        config.addDataSourceProperty("cacheResultSetMetadata", "true");
        config.addDataSourceProperty("cacheServerConfiguration", "true");
        config.addDataSourceProperty("elideSetAutoCommits", "true");
        config.addDataSourceProperty("maintainTimeStats", "false");
        
        // 连接池名称
        config.setPoolName("GDP-Job-Business-HikariCP");
        
        // 日志配置
        config.addDataSourceProperty("logger", "com.mysql.cj.log.Slf4JLogger");
        config.addDataSourceProperty("profileSQL", "true");
        
        logger.info("HikariCP configuration: maxPoolSize={}, minIdle={}, connectionTimeout={}ms", 
                   maximumPoolSize, minimumIdle, connectionTimeout);
        
        try {
            HikariDataSource dataSource = new HikariDataSource(config);
            logger.info("Successfully created custom datasource");
            return dataSource;
        } catch (Exception e) {
            logger.error("Failed to create custom datasource", e);
            throw new RuntimeException("Failed to create custom datasource", e);
        }
    }
    
    /**
     * 解密数据库密码
     * @param encryptedPassword 加密的密码
     * @return 解密后的密码
     */
    private String decryptPassword(String encryptedPassword) {
        try {
            if (AesDecryptor.isEncryptedValue(encryptedPassword)) {
                logger.info("Detected encrypted password, attempting to decrypt");
                String decryptedPassword = AesDecryptor.decrypt(encryptedPassword, AesDecryptor.getEncryptionKey());
                logger.info("Password decrypted successfully");
                return decryptedPassword;
            } else {
                logger.info("Using plain text password");
                return encryptedPassword;
            }
        } catch (Exception e) {
            logger.error("Failed to decrypt password, using original value", e);
            return encryptedPassword;
        }
    }
    
    /**
     * 获取数据源信息（用于调试）
     */
    public void logDataSourceInfo() {
        logger.info("=== DataSource Configuration ===");
        logger.info("URL: {}", url);
        logger.info("Username: {}", username);
        logger.info("Password: {}", AesDecryptor.isEncryptedValue(password) ? "***ENCRYPTED***" : "***PLAIN***");
        logger.info("Driver: {}", driverClassName);
        logger.info("Max Pool Size: {}", maximumPoolSize);
        logger.info("Min Idle: {}", minimumIdle);
        logger.info("Connection Timeout: {}ms", connectionTimeout);
        logger.info("Idle Timeout: {}ms", idleTimeout);
        logger.info("Max Lifetime: {}ms", maxLifetime);
        logger.info("================================");
    }
}
