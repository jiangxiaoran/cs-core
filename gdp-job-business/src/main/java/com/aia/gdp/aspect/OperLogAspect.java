package com.aia.gdp.aspect;

import com.aia.gdp.annotation.OperLog;
import com.aia.gdp.model.SysOperLog;
import com.aia.gdp.service.SysOperLogService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.Arrays;

/**
 * 操作日志切面
 * 拦截带有 @OperLog 注解的方法，自动记录操作日志
 */
@Aspect
@Component
public class OperLogAspect {

    private static final Logger logger = LoggerFactory.getLogger(OperLogAspect.class);

    @Autowired
    private SysOperLogService sysOperLogService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 环绕通知，记录方法执行时间和结果
     */
    @Around("@annotation(operLog)")
    public Object around(ProceedingJoinPoint joinPoint, OperLog operLog) throws Throwable {
        long startTime = System.currentTimeMillis();
        Object result = null;
        Exception exception = null;

        try {
            // 执行原方法
            result = joinPoint.proceed();
            return result;
        } catch (Exception e) {
            exception = e;
            throw e;
        } finally {
            // 记录操作日志
            try {
                recordOperLog(joinPoint, operLog, result, exception, System.currentTimeMillis() - startTime);
            } catch (Exception e) {
                logger.error("记录操作日志失败", e);
            }
        }
    }

    /**
     * 记录操作日志
     */
    private void recordOperLog(JoinPoint joinPoint, OperLog operLog, Object result, Exception exception, long costTime) {
        try {
            // 获取请求信息
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attributes == null) {
                logger.warn("无法获取请求上下文，跳过操作日志记录");
                return;
            }

            HttpServletRequest request = attributes.getRequest();
            MethodSignature signature = (MethodSignature) joinPoint.getSignature();
            Method method = signature.getMethod();

            // 创建操作日志对象
            SysOperLog sysOperLog = new SysOperLog();
            sysOperLog.setTitle(operLog.title());
            sysOperLog.setBusinessType(operLog.businessType());
            sysOperLog.setMethod(method.getDeclaringClass().getName() + "." + method.getName());
            sysOperLog.setRequestMethod(request.getMethod());
            sysOperLog.setOperatorType(SysOperLog.OperatorType.ADMIN); // 默认后台用户
            sysOperLog.setOperName(getCurrentUsername(request));
            sysOperLog.setDeptName(getCurrentDeptName(request));
            sysOperLog.setOperUrl(request.getRequestURI());
            sysOperLog.setOperIp(getClientIp(request));
            sysOperLog.setOperLocation(getLocationByIp(sysOperLog.getOperIp()));
            sysOperLog.setOperTime(LocalDateTime.now());
            sysOperLog.setCostTime(costTime);

            // 设置请求参数
            if (operLog.isSaveRequestData()) {
                try {
                    String operParam = objectMapper.writeValueAsString(joinPoint.getArgs());
                    // 限制参数长度，避免过长
                    if (operParam.length() > 2000) {
                        operParam = operParam.substring(0, 2000) + "...";
                    }
                    sysOperLog.setOperParam(operParam);
                } catch (JsonProcessingException e) {
                    sysOperLog.setOperParam("参数序列化失败: " + e.getMessage());
                }
            }

            // 设置响应结果
            if (operLog.isSaveResponseData() && result != null) {
                try {
                    String jsonResult = objectMapper.writeValueAsString(result);
                    // 限制结果长度，避免过长
                    if (jsonResult.length() > 2000) {
                        jsonResult = jsonResult.substring(0, 2000) + "...";
                    }
                    sysOperLog.setJsonResult(jsonResult);
                } catch (JsonProcessingException e) {
                    sysOperLog.setJsonResult("结果序列化失败: " + e.getMessage());
                }
            }

            // 设置操作状态和错误信息
            if (exception != null) {
                sysOperLog.setStatus(SysOperLog.Status.FAIL);
                String errorMsg = exception.getMessage();
                if (errorMsg != null && errorMsg.length() > 2000) {
                    errorMsg = errorMsg.substring(0, 2000);
                }
                sysOperLog.setErrorMsg(errorMsg);
            } else {
                sysOperLog.setStatus(SysOperLog.Status.SUCCESS);
            }

            // 保存操作日志
            sysOperLogService.recordOperLog(sysOperLog);
            logger.debug("操作日志记录成功: {}", sysOperLog.getTitle());

        } catch (Exception e) {
            logger.error("记录操作日志失败", e);
        }
    }

    /**
     * 获取当前用户名
     */
    private String getCurrentUsername(HttpServletRequest request) {
        // 从请求头或Session中获取用户名
        String username = request.getHeader("X-Username");
        if (username == null || username.trim().isEmpty()) {
            username = request.getHeader("Authorization");
            if (username != null && username.startsWith("Bearer ")) {
                // 这里可以解析JWT token获取用户名
                username = "admin"; // 临时默认值
            }
        }
        return username != null ? username : "unknown";
    }

    /**
     * 获取当前部门名称
     */
    private String getCurrentDeptName(HttpServletRequest request) {
        // 从请求头或Session中获取部门名称
        String deptName = request.getHeader("X-DeptName");
        return deptName != null ? deptName : "系统管理";
    }

    /**
     * 获取客户端IP地址
     */
    private String getClientIp(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("HTTP_CLIENT_IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("HTTP_X_FORWARDED_FOR");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        return ip;
    }

    /**
     * 根据IP获取地理位置（简化实现）
     */
    private String getLocationByIp(String ip) {
        // 这里可以集成IP地理位置服务
        // 暂时返回默认值
        if (ip != null && (ip.startsWith("127.") || ip.startsWith("192.168.") || ip.startsWith("10."))) {
            return "内网";
        }
        return "未知";
    }
}