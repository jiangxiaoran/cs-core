package com.aia.gdp.common;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 日期处理工具类
 * 提供日期占位符处理功能
 * 
 * @author andy
 * @date 2025-08-01
 * @company
 */
public class DateUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(DateUtils.class);
    
    /**
     * 处理日期占位符
     * 支持的占位符格式：
     * ${date} - 当前日期（默认格式 yyyyMMdd）
     * ${date:yyyy-MM-dd} - 指定格式的当前日期
     * ${now} - 当前时间戳（yyyyMMddHHmmss）
     * ${datetime:now} - 当前时间戳（yyyy-MM-dd HH:mm:ss）
     * ${today} - 当前日期（yyyyMMdd）
     * ${date:today} - 当前日期（yyyy-MM-dd）
     * ${yesterday} - 昨天日期（yyyyMMdd）
     * ${date:yesterday} - 昨天日期（yyyy-MM-dd）
     * ${lastMonth} - 上个月（yyyyMM）
     * ${yearmonth:lastMonth} - 上个月（yyyy-MM）
     * ${date+1d} - 明天日期
     * ${date-2d} - 前天日期
     * 
     * @param value 包含占位符的字符串
     * @return 处理后的字符串
     */
    public static String processDatePlaceholders(String value) {
        if (value == null || value.trim().isEmpty()) {
            return value;
        }
        
        LocalDate today = LocalDate.now();
        LocalDateTime now = LocalDateTime.now();
        
        // 定义正则表达式模式
        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(value);
        StringBuffer result = new StringBuffer();
        
        while (matcher.find()) {
            String placeholder = matcher.group(1);
            String replacement = processSinglePlaceholder(placeholder, today, now);
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        
        return result.toString();
    }
    
    /**
     * 处理单个占位符
     */
    private static String processSinglePlaceholder(String placeholder, LocalDate today, LocalDateTime now) {
        String[] parts = placeholder.split(":", 2);
        String type = parts[0].trim();
        String format = parts.length > 1 ? parts[1].trim() : null;
        
        switch (type) {
            case "date":
                return processDatePlaceholder(format, today);
            case "now":
                return now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            case "datetime":
                if ("now".equals(format)) {
                    return now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                }
                break;
            case "today":
                return today.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            case "yesterday":
                return today.minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            case "lastMonth":
                return today.minusMonths(1).format(DateTimeFormatter.ofPattern("yyyyMM"));
            case "yearmonth":
                if ("lastMonth".equals(format)) {
                    return today.minusMonths(1).format(DateTimeFormatter.ofPattern("yyyy-MM"));
                }
                break;
            default:
                // 处理加减天数的格式
                if (type.startsWith("date")) {
                    return processDateWithOffset(type, format, today);
                }
                break;
        }
        
        // 如果没有匹配到任何模式，返回原值
        return "${" + placeholder + "}";
    }
    
    /**
     * 处理日期占位符（带格式）
     */
    private static String processDatePlaceholder(String format, LocalDate date) {
        if (format == null || format.isEmpty()) {
            return date.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        }
        
        // 处理特殊格式
        switch (format) {
            case "today":
                return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            case "yesterday":
                return date.minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            default:
                try {
                    return date.format(DateTimeFormatter.ofPattern(format));
                } catch (Exception e) {
                    logger.warn("无效的日期格式: {}, 使用默认格式", format);
                    return date.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                }
        }
    }
    
    /**
     * 处理带偏移量的日期
     */
    private static String processDateWithOffset(String type, String format, LocalDate baseDate) {
        // 解析偏移量，如 date+1d, date-2d
        Pattern offsetPattern = Pattern.compile("date([+-]\\d+)d");
        Matcher offsetMatcher = offsetPattern.matcher(type);
        
        if (offsetMatcher.find()) {
            String offsetStr = offsetMatcher.group(1);
            int offset = Integer.parseInt(offsetStr);
            LocalDate targetDate = baseDate.plusDays(offset);
            
            if (format == null || format.isEmpty()) {
                return targetDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            } else {
                try {
                    return targetDate.format(DateTimeFormatter.ofPattern(format));
                } catch (Exception e) {
                    logger.warn("无效的日期格式: {}, 使用默认格式", format);
                    return targetDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                }
            }
        }
        
        return "${" + type + "}";
    }
}
