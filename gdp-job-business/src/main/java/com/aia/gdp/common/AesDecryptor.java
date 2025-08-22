package com.aia.gdp.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * AES 解密工具类
 * 用于解密配置文件中的加密密码
 *
 * @author andy
 * @date 2025-01-XX
 */
@Component
public class AesDecryptor {
    
    private static final Logger logger = LoggerFactory.getLogger(AesDecryptor.class);
    
    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/ECB/PKCS5Padding";
    private static final String ENCRYPTED_PREFIX = "ENC(";
    private static final String ENCRYPTED_SUFFIX = ")";
    
    /**
     * 解密字符串
     * @param encryptedValue 加密的值
     * @param key 解密密钥
     * @return 解密后的字符串
     */
    public static String decrypt(String encryptedValue, String key) {
        try {
            if (encryptedValue == null || !isEncryptedValue(encryptedValue)) {
                return encryptedValue;
            }
            
            String actualEncryptedValue = extractEncryptedValue(encryptedValue);
            
            SecretKeySpec secretKey = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), ALGORITHM);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            
            byte[] decryptedBytes = cipher.doFinal(Base64.getDecoder().decode(actualEncryptedValue));
            String decryptedValue = new String(decryptedBytes, StandardCharsets.UTF_8);
            
            logger.debug("Successfully decrypted value");
            return decryptedValue;
            
        } catch (Exception e) {
            logger.error("Failed to decrypt value: {}", encryptedValue, e);
            throw new RuntimeException("Failed to decrypt value: " + encryptedValue, e);
        }
    }
    
    /**
     * 加密字符串
     * @param value 要加密的值
     * @param key 加密密钥
     * @return 加密后的字符串（带ENC()前缀）
     */
    public static String encrypt(String value, String key) {
        try {
            SecretKeySpec secretKey = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), ALGORITHM);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            
            byte[] encryptedBytes = cipher.doFinal(value.getBytes(StandardCharsets.UTF_8));
            String encryptedValue = Base64.getEncoder().encodeToString(encryptedBytes);
            
            logger.debug("Successfully encrypted value");
            return ENCRYPTED_PREFIX + encryptedValue + ENCRYPTED_SUFFIX;
            
        } catch (Exception e) {
            logger.error("Failed to encrypt value", e);
            throw new RuntimeException("Failed to encrypt value", e);
        }
    }
    
    /**
     * 判断是否为加密值
     * @param value 要检查的值
     * @return 是否为加密值
     */
    public static boolean isEncryptedValue(String value) {
        return value != null && 
               value.startsWith(ENCRYPTED_PREFIX) && 
               value.endsWith(ENCRYPTED_SUFFIX);
    }
    
    /**
     * 提取加密值（去掉ENC()前缀和后缀）
     * @param encryptedValue 加密值
     * @return 纯加密字符串
     */
    public static String extractEncryptedValue(String encryptedValue) {
        if (!isEncryptedValue(encryptedValue)) {
            throw new IllegalArgumentException("Not a valid encrypted value: " + encryptedValue);
        }
        
        return encryptedValue.substring(
            ENCRYPTED_PREFIX.length(),
            encryptedValue.length() - ENCRYPTED_SUFFIX.length()
        );
    }
    
    /**
     * 获取加密密钥
     * 优先级：环境变量 > 系统属性 > 默认值
     * @return 加密密钥
     */
    public static String getEncryptionKey() {
        String key = System.getenv("DB_ENCRYPTION_KEY");
        if (key != null && !key.trim().isEmpty()) {
            logger.debug("Using encryption key from environment variable");
            return key;
        }
        
        key = System.getProperty("db.encryption.key");
        if (key != null && !key.trim().isEmpty()) {
            logger.debug("Using encryption key from system property");
            return key;
        }
        
        logger.warn("No encryption key found in environment variables or system properties, using default key");
        return "#d3@e46ssd43muddhln42iswk84rer32";
    }
}
