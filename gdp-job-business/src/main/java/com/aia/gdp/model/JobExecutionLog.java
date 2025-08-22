package com.aia.gdp.model;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.IdType;
import java.util.Date;

@TableName("job_execution_log")
public class JobExecutionLog {
    @TableId(value = "log_id", type = IdType.AUTO)
    private Long logId;
    private String jobCode;
    private String batchNo;
    private String executorProc;
    private String executorAddress;
    private Date startTime;
    private Date endTime;
    private String status;
    private String errorMessage;
    private Integer duration;
    private String notifyStatus;
    private Integer retryCount;
    private Date createTime;

    public Long getLogId() { return logId; }
    public void setLogId(Long logId) { this.logId = logId; }
    public String getJobCode() { return jobCode; }
    public void setJobCode(String jobCode) { this.jobCode = jobCode; }
    public String getBatchNo() { return batchNo; }
    public void setBatchNo(String batchNo) { this.batchNo = batchNo; }
    public String getExecutorProc() { return executorProc; }
    public void setExecutorProc(String executorProc) { this.executorProc = executorProc; }
    public String getExecutorAddress() { return executorAddress; }
    public void setExecutorAddress(String executorAddress) { this.executorAddress = executorAddress; }
    public Date getStartTime() { return startTime; }
    public void setStartTime(Date startTime) { this.startTime = startTime; }
    public Date getEndTime() { return endTime; }
    public void setEndTime(Date endTime) { this.endTime = endTime; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public Integer getDuration() { return duration; }
    public void setDuration(Integer duration) { this.duration = duration; }
    public String getNotifyStatus() { return notifyStatus; }
    public void setNotifyStatus(String notifyStatus) { this.notifyStatus = notifyStatus; }
    public Integer getRetryCount() { return retryCount; }
    public void setRetryCount(Integer retryCount) { this.retryCount = retryCount; }
    public Date getCreateTime() { return createTime; }
    public void setCreateTime(Date createTime) { this.createTime = createTime; }
} 