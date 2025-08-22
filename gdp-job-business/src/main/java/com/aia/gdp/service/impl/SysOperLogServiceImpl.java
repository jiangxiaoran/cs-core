package com.aia.gdp.service.impl;

import com.aia.gdp.mapper.SysOperLogMapper;
import com.aia.gdp.model.SysOperLog;
import com.aia.gdp.service.SysOperLogService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * 系统操作日志服务实现类
 */
@Service
public class SysOperLogServiceImpl implements SysOperLogService {
    
    private static final Logger logger = LoggerFactory.getLogger(SysOperLogServiceImpl.class);
    
    @Autowired
    private SysOperLogMapper sysOperLogMapper;
    
    @Override
    public void recordOperLog(SysOperLog operLog) {
        try {
            if (operLog.getOperTime() == null) {
                operLog.setOperTime(LocalDateTime.now());
            }
            sysOperLogMapper.insert(operLog);
            logger.debug("操作日志记录成功: {}", operLog.getTitle());
        } catch (Exception e) {
            logger.error("记录操作日志失败: {}", operLog.getTitle(), e);
        }
    }

    @Override
    public IPage<SysOperLog> getOperLogPage(Page<SysOperLog> page, String title, 
                                           Integer businessType, String operName, 
                                           Integer status, String startTime, String endTime) {
        QueryWrapper<SysOperLog> queryWrapper = new QueryWrapper<>();
        
        if (title != null && !title.trim().isEmpty()) {
            queryWrapper.like("title", title);
        }
        if (businessType != null) {
            queryWrapper.eq("business_type", businessType);
        }
        if (operName != null && !operName.trim().isEmpty()) {
            queryWrapper.like("oper_name", operName);
        }
        if (status != null) {
            queryWrapper.eq("status", status);
        }
        if (startTime != null && !startTime.trim().isEmpty()) {
            queryWrapper.ge("oper_time", startTime);
        }
        if (endTime != null && !endTime.trim().isEmpty()) {
            queryWrapper.le("oper_time", endTime);
        }
        
        queryWrapper.orderByDesc("oper_time");
        
        return sysOperLogMapper.selectPage(page, queryWrapper);
    }

    @Override
    public SysOperLog getOperLogById(Long operId) {
        return sysOperLogMapper.selectById(operId);
    }

    @Override
    public boolean deleteOperLog(Long operId) {
        return sysOperLogMapper.deleteById(operId) > 0;
    }

    @Override
    public boolean batchDeleteOperLog(List<Long> operIds) {
        return sysOperLogMapper.deleteBatchIds(operIds) > 0;
    }

    @Override
    public boolean clearOperLog() {
        QueryWrapper<SysOperLog> queryWrapper = new QueryWrapper<>();
        return sysOperLogMapper.delete(queryWrapper) > 0;
    }

    @Override
    public List<SysOperLog> exportOperLog(String title, Integer businessType, 
                                         String operName, Integer status, 
                                         String startTime, String endTime) {
        QueryWrapper<SysOperLog> queryWrapper = new QueryWrapper<>();
        
        if (title != null && !title.trim().isEmpty()) {
            queryWrapper.like("title", title);
        }
        if (businessType != null) {
            queryWrapper.eq("business_type", businessType);
        }
        if (operName != null && !operName.trim().isEmpty()) {
            queryWrapper.like("oper_name", operName);
        }
        if (status != null) {
            queryWrapper.eq("status", status);
        }
        if (startTime != null && !startTime.trim().isEmpty()) {
            queryWrapper.ge("oper_time", startTime);
        }
        if (endTime != null && !endTime.trim().isEmpty()) {
            queryWrapper.le("oper_time", endTime);
        }
        
        queryWrapper.orderByDesc("oper_time");
        
        return sysOperLogMapper.selectList(queryWrapper);
    }
    
    /**
     * 记录用户登录操作
     */
    public void recordLogin(String username, String ip, String userAgent, boolean success, String errorMsg) {
        String title = success ? "用户登录" : "用户登录失败";
        String method = "com.aia.gdp.auth.AuthService.login";
        String operParam = String.format("username=%s, ip=%s, userAgent=%s", username, ip, userAgent);
        String jsonResult = success ? "登录成功" : null;
        
        SysOperLog operLog = new SysOperLog();
        operLog.setTitle(title);
        operLog.setBusinessType(SysOperLog.BusinessType.OTHER);
        operLog.setMethod(method);
        operLog.setOperName(username);
        operLog.setOperParam(operParam);
        operLog.setJsonResult(jsonResult);
        operLog.setStatus(success ? SysOperLog.Status.SUCCESS : SysOperLog.Status.FAIL);
        operLog.setErrorMsg(errorMsg);
        operLog.setOperTime(LocalDateTime.now());
        operLog.setOperIp(ip);
        operLog.setRequestMethod("POST");
        operLog.setOperatorType(SysOperLog.OperatorType.ADMIN);
        
        recordOperLog(operLog);
    }
    
    /**
     * 记录用户登出操作
     */
    public void recordLogout(String username, String ip) {
        String title = "用户登出";
        String method = "com.aia.gdp.auth.AuthService.logout";
        String operParam = String.format("username=%s, ip=%s", username, ip);
        
        SysOperLog operLog = new SysOperLog();
        operLog.setTitle(title);
        operLog.setBusinessType(SysOperLog.BusinessType.OTHER);
        operLog.setMethod(method);
        operLog.setOperName(username);
        operLog.setOperParam(operParam);
        operLog.setJsonResult("登出成功");
        operLog.setStatus(SysOperLog.Status.SUCCESS);
        operLog.setOperTime(LocalDateTime.now());
        operLog.setOperIp(ip);
        operLog.setRequestMethod("POST");
        operLog.setOperatorType(SysOperLog.OperatorType.ADMIN);
        
        recordOperLog(operLog);
    }
    
    /**
     * 记录任务控制操作
     */
    public void recordJobControl(String operation, String target, String operator, 
                               String params, boolean success, String errorMsg) {
        String title = operation;
        String method = "com.aia.gdp.service.JobControlService." + operation.toLowerCase();
        
        SysOperLog operLog = new SysOperLog();
        operLog.setTitle(title);
        operLog.setBusinessType(SysOperLog.BusinessType.UPDATE);
        operLog.setMethod(method);
        operLog.setOperName(operator);
        operLog.setOperParam(params);
        operLog.setJsonResult(success ? "操作成功" : "操作失败");
        operLog.setStatus(success ? SysOperLog.Status.SUCCESS : SysOperLog.Status.FAIL);
        operLog.setErrorMsg(errorMsg);
        operLog.setOperTime(LocalDateTime.now());
        operLog.setRequestMethod("POST");
        operLog.setOperatorType(SysOperLog.OperatorType.ADMIN);
        
        recordOperLog(operLog);
    }
}