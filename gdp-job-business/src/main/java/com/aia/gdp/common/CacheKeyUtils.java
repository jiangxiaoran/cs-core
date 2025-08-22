package com.aia.gdp.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.Objects;

/**
 * 缓存键值工具类
 * 
 * 功能特性：
 * - 统一管理缓存键值的生成和解析
 * - 支持作业级别和作业组级别的键值生成
 * - 提供键值验证和错误处理
 * - 支持批次级别的数据管理
 * 
 * 键值格式规范：
 * - 作业键值：jobCode_batchNo
 * - 作业组键值：groupName_batchNo
 * - 批次键值：batchNo_timestamp
 * 
 * @author andy
 * @date 2025-08-18
 * @version 1.0
 */
public class CacheKeyUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(CacheKeyUtils.class);
    
    private static final String KEY_SEPARATOR = "$";
    private static final String INVALID_KEY_PLACEHOLDER = "INVALID";
    
    /**
     * 生成作业缓存键
     * 
     * @param jobCode 作业代码
     * @param batchNo 批次号
     * @return 格式化的作业缓存键
     * @throws IllegalArgumentException 如果参数无效
     */
    public static String generateJobKey(String jobCode, String batchNo) {
        validateJobKeyParams(jobCode, batchNo);
        
        String jobKey = jobCode + KEY_SEPARATOR + batchNo;
        logger.debug("生成作业缓存键: {} -> {}", String.format("jobCode=%s, batchNo=%s", jobCode, batchNo), jobKey);
        
        return jobKey;
    }
    
    /**
     * 生成作业组缓存键
     * 
     * @param groupName 作业组名称
     * @param batchNo 批次号
     * @return 格式化的作业组缓存键
     * @throws IllegalArgumentException 如果参数无效
     */
    public static String generateGroupKey(String groupName, String batchNo) {
        validateGroupKeyParams(groupName, batchNo);
        
        String groupKey = groupName + KEY_SEPARATOR + batchNo;
        logger.debug("生成作业组缓存键: {} -> {}", String.format("groupName=%s, batchNo=%s", groupName, batchNo), groupKey);
        
        return groupKey;
    }
    
    /**
     * 生成批次键值（用于批次级别的操作）
     * 
     * @param batchNo 批次号
     * @return 格式化的批次键值
     * @throws IllegalArgumentException 如果参数无效
     */
    public static String generateBatchKey(String batchNo) {
        validateBatchNo(batchNo);
        
        String batchKey = batchNo + KEY_SEPARATOR + System.currentTimeMillis();
        logger.debug("生成批次键值: {} -> {}", batchNo, batchKey);
        
        return batchKey;
    }
    
    /**
     * 从键值中提取作业代码
     * 
     * @param jobKey 作业缓存键
     * @return 作业代码，如果键值无效则返回空字符串
     */
    public static String extractJobCode(String jobKey) {
        if (!isValidJobKey(jobKey)) {
            logger.warn("无效的作业键值: {}", jobKey);
            return "";
        }
        
        int lastUnderscoreIndex = jobKey.lastIndexOf(KEY_SEPARATOR);
        String jobCode = lastUnderscoreIndex > 0 ? jobKey.substring(0, lastUnderscoreIndex) : jobKey;
        
        logger.debug("从作业键值提取作业代码: {} -> {}", jobKey, jobCode);
        return jobCode;
    }
    
    /**
     * 从键值中提取批次号
     * 
     * @param key 缓存键值
     * @return 批次号，如果键值无效则返回空字符串
     */
    public static String extractBatchNo(String key) {
        if (!StringUtils.hasText(key)) {
            logger.warn("键值为空，无法提取批次号");
            return "";
        }
        
        int lastUnderscoreIndex = key.lastIndexOf(KEY_SEPARATOR);
        String batchNo = lastUnderscoreIndex > 0 ? key.substring(lastUnderscoreIndex + 1) : "";
        
        logger.debug("从键值提取批次号: {} -> {}", key, batchNo);
        return batchNo;
    }
    
    /**
     * 从键值中提取组名
     * 
     * @param groupKey 作业组缓存键
     * @return 组名，如果键值无效则返回空字符串
     */
    public static String extractGroupName(String groupKey) {
        if (!isValidGroupKey(groupKey)) {
            logger.warn("无效的作业组键值: {}", groupKey);
            return "";
        }
        
        int lastUnderscoreIndex = groupKey.lastIndexOf(KEY_SEPARATOR);
        String groupName = lastUnderscoreIndex > 0 ? groupKey.substring(0, lastUnderscoreIndex) : groupKey;
        
        logger.debug("从作业组键值提取组名: {} -> {}", groupKey, groupName);
        return groupName;
    }
    
    /**
     * 验证键值是否属于指定批次
     * 
     * @param key 缓存键值
     * @param batchNo 批次号
     * @return 如果键值属于指定批次则返回true
     */
    public static boolean isKeyBelongsToBatch(String key, String batchNo) {
        if (!StringUtils.hasText(key) || !StringUtils.hasText(batchNo)) {
            return false;
        }
        
        String extractedBatchNo = extractBatchNo(key);
        boolean belongs = batchNo.equals(extractedBatchNo);
        
        logger.debug("验证键值批次归属: {} -> {} (期望: {})", key, extractedBatchNo, batchNo);
        return belongs;
    }
    
    /**
     * 验证作业键值是否有效
     * 
     * @param jobKey 作业缓存键
     * @return 如果键值有效则返回true
     */
    public static boolean isValidJobKey(String jobKey) {
        if (!StringUtils.hasText(jobKey)) {
            return false;
        }
        
        int underscoreCount = countOccurrences(jobKey, KEY_SEPARATOR);
        boolean isValid = underscoreCount == 1 && jobKey.length() > 2;
        
        if (!isValid) {
            logger.debug("作业键值格式无效: {} (下划线数量: {})", jobKey, underscoreCount);
        }
        
        return isValid;
    }
    
    /**
     * 验证作业组键值是否有效
     * 
     * @param groupKey 作业组缓存键
     * @return 如果键值有效则返回true
     */
    public static boolean isValidGroupKey(String groupKey) {
        if (!StringUtils.hasText(groupKey)) {
            return false;
        }
        
        int underscoreCount = countOccurrences(groupKey, KEY_SEPARATOR);
        boolean isValid = underscoreCount == 1 && groupKey.length() > 2;
        
        if (!isValid) {
            logger.debug("作业组键值格式无效: {} (下划线数量: {})", groupKey, underscoreCount);
        }
        
        return isValid;
    }
    
    /**
     * 生成安全的键值（防止无效参数导致异常）
     * 
     * @param jobCode 作业代码
     * @param batchNo 批次号
     * @return 安全的作业缓存键，如果参数无效则返回占位符
     */
    public static String generateSafeJobKey(String jobCode, String batchNo) {
        try {
            return generateJobKey(jobCode, batchNo);
        } catch (Exception e) {
            logger.error("生成作业键值失败，使用占位符: jobCode={}, batchNo={}", jobCode, batchNo, e);
            return INVALID_KEY_PLACEHOLDER + KEY_SEPARATOR + System.currentTimeMillis();
        }
    }
    
    /**
     * 生成安全的组键值（防止无效参数导致异常）
     * 
     * @param groupName 作业组名称
     * @param batchNo 批次号
     * @return 安全的作业组缓存键，如果参数无效则返回占位符
     */
    public static String generateSafeGroupKey(String groupName, String batchNo) {
        try {
            return generateGroupKey(groupName, batchNo);
        } catch (Exception e) {
            logger.error("生成作业组键值失败，使用占位符: groupName={}, batchNo={}", groupName, batchNo, e);
            return INVALID_KEY_PLACEHOLDER + KEY_SEPARATOR + System.currentTimeMillis();
        }
    }
    
    /**
     * 验证作业键值参数
     * 
     * @param jobCode 作业代码
     * @param batchNo 批次号
     * @throws IllegalArgumentException 如果参数无效
     */
    private static void validateJobKeyParams(String jobCode, String batchNo) {
        if (!StringUtils.hasText(jobCode)) {
            throw new IllegalArgumentException("作业代码不能为空");
        }
        if (!StringUtils.hasText(batchNo)) {
            throw new IllegalArgumentException("批次号不能为空");
        }
        if (jobCode.contains(KEY_SEPARATOR)) {
            throw new IllegalArgumentException("作业代码不能包含下划线字符: " + jobCode);
        }
        if (batchNo.contains(KEY_SEPARATOR)) {
            throw new IllegalArgumentException("批次号不能包含下划线字符: " + batchNo);
        }
    }
    
    /**
     * 验证作业组键值参数
     * 
     * @param groupName 作业组名称
     * @param batchNo 批次号
     * @throws IllegalArgumentException 如果参数无效
     */
    private static void validateGroupKeyParams(String groupName, String batchNo) {
        if (!StringUtils.hasText(groupName)) {
            throw new IllegalArgumentException("作业组名称不能为空");
        }
        if (!StringUtils.hasText(batchNo)) {
            throw new IllegalArgumentException("批次号不能为空");
        }
        if (groupName.contains(KEY_SEPARATOR)) {
            throw new IllegalArgumentException("作业组名称不能包含下划线字符: " + groupName);
        }
        if (batchNo.contains(KEY_SEPARATOR)) {
            throw new IllegalArgumentException("批次号不能包含下划线字符: " + batchNo);
        }
    }
    
    /**
     * 验证批次号
     * 
     * @param batchNo 批次号
     * @throws IllegalArgumentException 如果参数无效
     */
    private static void validateBatchNo(String batchNo) {
        if (!StringUtils.hasText(batchNo)) {
            throw new IllegalArgumentException("批次号不能为空");
        }
        if (batchNo.contains(KEY_SEPARATOR)) {
            throw new IllegalArgumentException("批次号不能包含下划线字符: " + batchNo);
        }
    }
    
    /**
     * 计算字符串中指定字符的出现次数
     * 
     * @param str 目标字符串
     * @param target 目标字符
     * @return 出现次数
     */
    private static int countOccurrences(String str, String target) {
        if (!StringUtils.hasText(str) || !StringUtils.hasText(target)) {
            return 0;
        }
        
        int count = 0;
        int lastIndex = 0;
        
        while (lastIndex != -1) {
            lastIndex = str.indexOf(target, lastIndex);
            if (lastIndex != -1) {
                count++;
                lastIndex += target.length();
            }
        }
        
        return count;
    }
}
