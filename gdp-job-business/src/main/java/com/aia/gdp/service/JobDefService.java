package com.aia.gdp.service;

import com.aia.gdp.model.JobDef;
import java.util.List;

public interface JobDefService {
    List<JobDef> getJobsByGroupOrdered(String jobGroup);
    List<JobDef> list();
    JobDef getById(Long jobId);
    JobDef getJobByCode(String jobCode);
    void save(JobDef jobDef);
    void update(JobDef jobDef);
    void deleteById(Long jobId);
} 