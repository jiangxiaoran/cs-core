package com.aia.gdp.model;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.TableField;
import java.util.Date;

@TableName("job_def")
public class JobDef {
    @TableId("job_id")
    private Long jobId;
    
    @TableField("job_code")
    private String jobCode;
    
    @TableField("job_name")
    private String jobName;
    
    @TableField("proc_name")
    private String procName;
    
    @TableField("job_type")
    private String jobType;
    
    @TableField("schedule_time")
    private java.sql.Time scheduleTime;
    
    // 用于接收前端发送的时间字符串，不映射到数据库
    @TableField(exist = false)
    private String scheduleTimeStr;
    
    @TableField("status")
    private String status;
    
    @TableField("timeout_sec")
    private Integer timeoutSec;
    
    @TableField("retry_count")
    private Integer retryCount;
    
    @TableField("notify_email")
    private String notifyEmail;
    
    @TableField("is_depend")
    private Boolean isDepend;
    
    @TableField("is_active")
    private Boolean isActive;
    
    @TableField("last_run_time")
    private Date lastRunTime;
    
    @TableField("next_run_time")
    private Date nextRunTime;
    
    @TableField("create_time")
    private Date createTime;
    
    @TableField("update_time")
    private Date updateTime;
    
    @TableField("job_group")
    private String jobGroup;
    
    @TableField("job_order")
    private Integer jobOrder;
    
    @TableField("group_order")
    private Integer groupOrder;
    
    @TableField("job_params")
    private String jobParams;

    public Long getJobId() { return jobId; }
    public void setJobId(Long jobId) { this.jobId = jobId; }
    public String getJobCode() { return jobCode; }
    public void setJobCode(String jobCode) { this.jobCode = jobCode; }
    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }
    public String getProcName() { return procName; }
    public void setProcName(String procName) { this.procName = procName; }
    public String getJobType() { return jobType; }
    public void setJobType(String jobType) { this.jobType = jobType; }
    public java.sql.Time getScheduleTime() { return scheduleTime; }
    public void setScheduleTime(java.sql.Time scheduleTime) { this.scheduleTime = scheduleTime; }
    public String getScheduleTimeStr() { return scheduleTimeStr; }
    public void setScheduleTimeStr(String scheduleTimeStr) { this.scheduleTimeStr = scheduleTimeStr; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public Integer getTimeoutSec() { return timeoutSec; }
    public void setTimeoutSec(Integer timeoutSec) { this.timeoutSec = timeoutSec; }
    public Integer getRetryCount() { return retryCount; }
    public void setRetryCount(Integer retryCount) { this.retryCount = retryCount; }
    public String getNotifyEmail() { return notifyEmail; }
    public void setNotifyEmail(String notifyEmail) { this.notifyEmail = notifyEmail; }
    public Boolean getIsDepend() { return isDepend; }
    public void setIsDepend(Boolean isDepend) { this.isDepend = isDepend; }
    public Boolean getIsActive() { return isActive; }
    public void setIsActive(Boolean isActive) { this.isActive = isActive; }
    public Date getLastRunTime() { return lastRunTime; }
    public void setLastRunTime(Date lastRunTime) { this.lastRunTime = lastRunTime; }
    public Date getNextRunTime() { return nextRunTime; }
    public void setNextRunTime(Date nextRunTime) { this.nextRunTime = nextRunTime; }
    public Date getCreateTime() { return createTime; }
    public void setCreateTime(Date createTime) { this.createTime = createTime; }
    public Date getUpdateTime() { return updateTime; }
    public void setUpdateTime(Date updateTime) { this.updateTime = updateTime; }
    public String getJobGroup() { return jobGroup; }
    public void setJobGroup(String jobGroup) { this.jobGroup = jobGroup; }
    public Integer getJobOrder() { return jobOrder; }
    public void setJobOrder(Integer jobOrder) { this.jobOrder = jobOrder; }
    public Integer getGroupOrder() { return groupOrder; }
    public void setGroupOrder(Integer groupOrder) { this.groupOrder = groupOrder; }
    public String getJobParams() { return jobParams; }
    public void setJobParams(String jobParams) { this.jobParams = jobParams; }
} 