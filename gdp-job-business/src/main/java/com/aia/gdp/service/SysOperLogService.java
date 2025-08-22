package com.aia.gdp.service;

import com.aia.gdp.model.SysOperLog;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

import java.util.List;

/**
 * 操作日志服务接口
 */
public interface SysOperLogService {

    /**
     * 记录操作日志
     */
    void recordOperLog(SysOperLog operLog);

    /**
     * 分页查询操作日志
     */
    IPage<SysOperLog> getOperLogPage(Page<SysOperLog> page, String title, 
                                    Integer businessType, String operName, 
                                    Integer status, String startTime, String endTime);

    /**
     * 根据ID查询操作日志
     */
    SysOperLog getOperLogById(Long operId);

    /**
     * 删除操作日志
     */
    boolean deleteOperLog(Long operId);

    /**
     * 批量删除操作日志
     */
    boolean batchDeleteOperLog(List<Long> operIds);

    /**
     * 清空操作日志
     */
    boolean clearOperLog();

    /**
     * 导出操作日志
     */
    List<SysOperLog> exportOperLog(String title, Integer businessType, 
                                  String operName, Integer status, 
                                  String startTime, String endTime);
}