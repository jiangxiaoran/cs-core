package com.aia.gdp.mapper;

import com.aia.gdp.model.JobExecutionLog;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import java.util.List;

@Mapper
public interface JobExecutionLogMapper extends BaseMapper<JobExecutionLog> {
    List<JobExecutionLog> selectByJobCode(@Param("jobCode") String jobCode);
    List<JobExecutionLog> selectByBatchNo(@Param("batchNo") String batchNo);
    List<JobExecutionLog> selectAll();
    JobExecutionLog selectById(@Param("logId") Long logId);
    int insert(JobExecutionLog log);
    int update(JobExecutionLog log);
    int deleteById(@Param("logId") Long logId);
} 