package com.aia.gdp.mapper;

import com.aia.gdp.model.JobDef;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import java.util.List;

@Mapper
public interface JobDefMapper extends BaseMapper<JobDef> {
    List<JobDef> selectByJobGroupOrder(@Param("jobGroup") String jobGroup);
    List<JobDef> selectAll();
    JobDef selectById(@Param("jobId") Long jobId);
    int insert(JobDef jobDef);
    int update(JobDef jobDef);
    int deleteById(@Param("jobId") Long jobId);
    
    /**
     * 分页查询任务列表
     */
    IPage<JobDef> selectPage(Page<JobDef> page, @Param("ew") com.baomidou.mybatisplus.core.conditions.Wrapper<JobDef> queryWrapper);
    
    /**
     * 根据任务代码查询任务
     */
    JobDef selectByJobCode(@Param("jobCode") String jobCode);
} 