package com.aia.gdp.common;

import java.util.Date;

/**
 * 通用工具类
 * 提供常用的通用处理功能
 *
 * @author andy
 * @date 2025-08-02
 * @company
 */
public class Utils {
    /**
     * 生成批次号
     * 使用当前时间戳作为批次号
     */
    public static String generateBatchNo() {
        return "BATCH-" + java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"));
    }
    /**
     * 生成批次号
     */
    public static String generateBatchNo(Long jobId) {
        String dateStr = new java.text.SimpleDateFormat("yyyyMMdd").format(new Date());
        String timeStr = new java.text.SimpleDateFormat("HHmmss").format(new Date());
        return String.format("BATCH%03d-%s-%s", jobId, dateStr, timeStr);
    }

}
