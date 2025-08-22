package com.aia.gdp.common;

import java.util.HashMap;
import java.util.Map;

/**
 * 统一API响应格式
 * 
 * @author andy
 * @date 2025-08-06
 */
public class ApiResponse<T> {
    private Integer code;
    private String msg;
    private T data;
    private Long timestamp;
    private Boolean success;

    public ApiResponse() {
        this.timestamp = System.currentTimeMillis();
    }

    public ApiResponse(Integer code, String msg, T data, Boolean success) {
        this.code = code;
        this.msg = msg;
        this.data = data;
        this.success = success;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * 成功响应
     */
    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(200, "操作成功", data, true);
    }

    /**
     * 成功响应（无数据）
     */
    public static <T> ApiResponse<T> success() {
        return new ApiResponse<>(200, "操作成功", null, true);
    }

    /**
     * 成功响应（自定义消息）
     */
    public static <T> ApiResponse<T> success(String msg, T data) {
        return new ApiResponse<>(200, msg, data, true);
    }

    /**
     * 失败响应
     */
    public static <T> ApiResponse<T> error(Integer code, String msg) {
        return new ApiResponse<>(code, msg, null, false);
    }

    /**
     * 失败响应（默认错误码）
     */
    public static <T> ApiResponse<T> error(String msg) {
        return new ApiResponse<>(500, msg, null, false);
    }

    /**
     * 分页响应
     */
    public static <T> ApiResponse<Map<String, Object>> page(T list, Integer total, Integer current, Integer pageSize) {
        Map<String, Object> pageData = new HashMap<>();
        pageData.put("list", list);
        pageData.put("total", total);
        pageData.put("current", current);
        pageData.put("pageSize", pageSize);
        pageData.put("pages", (total + pageSize - 1) / pageSize);
        
        return new ApiResponse<>(200, "获取成功", pageData, true);
    }

    // Getters and Setters
    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }
} 