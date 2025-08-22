package com.aia.gdp.service.impl;

import com.aia.gdp.model.JobDef;
import com.aia.gdp.mapper.JobDefMapper;
import com.aia.gdp.service.JobDefService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.*;
/**
 * 作业任务服务实现类
 *
 * @author andy
 * @date
 * @company
 */
@Service
public class JobDefServiceImpl implements JobDefService {
    @Autowired
    private JobDefMapper jobDefMapper;

    @Override
    public List<JobDef> getJobsByGroupOrdered(String jobGroup) {
        // 保持原有的业务逻辑：按组获取并按顺序排序
        List<JobDef> jobs = jobDefMapper.selectByJobGroupOrder(jobGroup);
        // 如果数据库查询没有排序，在这里进行排序
        jobs.sort(Comparator.comparing(JobDef::getGroupOrder, Comparator.nullsLast(Integer::compareTo))
                .thenComparing(JobDef::getJobOrder, Comparator.nullsLast(Integer::compareTo)));
        return jobs;
    }

    @Override
    public List<JobDef> list() {
        // 使用MyBatis-Plus的selectList方法
        return jobDefMapper.selectList(null);
    }

    @Override
    public JobDef getById(Long jobId) {
        // 使用MyBatis-Plus的selectById方法
        return jobDefMapper.selectById(jobId);
    }

    @Override
    public JobDef getJobByCode(String jobCode) {
        // 使用MyBatis-Plus的条件查询方法
        return jobDefMapper.selectOne(
            new com.baomidou.mybatisplus.core.conditions.query.QueryWrapper<JobDef>()
                .eq("job_code", jobCode)
        );
    }

    @Override
    public void save(JobDef jobDef) {
        // 确保设置创建和更新时间
        if (jobDef.getCreateTime() == null) {
            jobDef.setCreateTime(new java.util.Date());
        }
        if (jobDef.getUpdateTime() == null) {
            jobDef.setUpdateTime(new java.util.Date());
        }
        // 使用自定义的insert方法，确保job_params字段正确处理
        jobDefMapper.insert(jobDef);
    }

    @Override
    public void update(JobDef jobDef) {
        // 确保设置更新时间
        jobDef.setUpdateTime(new java.util.Date());
        // 使用自定义的update方法，确保job_params字段正确处理
        jobDefMapper.update(jobDef);
    }

    @Override
    public void deleteById(Long jobId) {
        // 使用MyBatis-Plus的deleteById方法
        jobDefMapper.deleteById(jobId);
    }
} 