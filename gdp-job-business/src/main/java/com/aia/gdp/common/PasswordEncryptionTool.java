package com.aia.gdp.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 密码加密工具类
 * 用于生成加密的数据库密码
 * 注意：此工具类仅用于开发环境生成加密密码，生产环境请删除
 *
 * @author andy
 * @date 2025-01-XX
 */
public class PasswordEncryptionTool {
    
    private static final Logger logger = LoggerFactory.getLogger(PasswordEncryptionTool.class);
    
    /**
     * 主方法，用于生成加密密码
     * 使用方法：java -cp target/classes com.aia.gdp.util.PasswordEncryptionTool <原始密码> <加密密钥>
     */
    public static void main(String[] args) {
        /* 
        if (args.length < 2) {
            logger.error("使用方法: java -cp target/classes com.aia.gdp.util.PasswordEncryptionTool <原始密码> <加密密钥>");
            logger.error("示例: java -cp target/classes com.aia.gdp.util.PasswordEncryptionTool zxr_123456 my-secret-key");
            System.exit(1);
        }
        
        String originalPassword = args[0];
        String encryptionKey = args[1];
        */
        String originalPassword = "G4s9$NX2H)_#Ht";
        String encryptionKey = "#d3@e46ssd43muddhln42iswk84rer32";
        try {
            String encryptedPassword = AesDecryptor.encrypt(originalPassword, encryptionKey);
            logger.info("=== 密码加密结果 ===");
            logger.info("原始密码: {}", originalPassword);
            logger.info("加密密钥: {}", encryptionKey);
            logger.info("加密结果: {}", encryptedPassword);
            logger.info("配置文件使用: spring.datasource.password={}", encryptedPassword);
            logger.info("====================");
            
            // 验证解密
            String decryptedPassword = AesDecryptor.decrypt(encryptedPassword, encryptionKey);
            if (originalPassword.equals(decryptedPassword)) {
                logger.info("✓ 加密解密验证成功");
            } else {
                logger.error("✗ 加密解密验证失败");
            }
            
        } catch (Exception e) {
            logger.error("密码加密失败", e);
            System.exit(1);
        }
    }
    
    /**
     * 加密密码（静态方法）
     * @param originalPassword 原始密码
     * @param encryptionKey 加密密钥
     * @return 加密后的密码
     */
    public static String encryptPassword(String originalPassword, String encryptionKey) {
        try {
            return AesDecryptor.encrypt(originalPassword, encryptionKey);
        } catch (Exception e) {
            logger.error("密码加密失败", e);
            throw new RuntimeException("密码加密失败", e);
        }
    }
    
    /**
     * 生成配置文件示例
     */
    public static void generateConfigExample() {
        logger.info("=== 配置文件示例 ===");
        logger.info("# 数据库配置");
        logger.info("spring.datasource.url=jdbc:mysql://localhost:3306/grm_system?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true");
        logger.info("spring.datasource.username=root");
        logger.info("spring.datasource.password=ENC(AES:your-encrypted-password-here)");
        logger.info("spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver");
        logger.info("");
        logger.info("# 连接池配置");
        logger.info("spring.datasource.hikari.maximum-pool-size=20");
        logger.info("spring.datasource.hikari.minimum-idle=5");
        logger.info("spring.datasource.hikari.connection-timeout=30000");
        logger.info("spring.datasource.hikari.idle-timeout=600000");
        logger.info("spring.datasource.hikari.max-lifetime=1800000");
        logger.info("spring.datasource.hikari.leak-detection-threshold=60000");
        logger.info("spring.datasource.hikari.validation-timeout=5000");
        logger.info("");
        logger.info("# 环境变量设置");
        logger.info("export DB_ENCRYPTION_KEY=\"your-secret-key-here\"");
        logger.info("==================");
    }
}
