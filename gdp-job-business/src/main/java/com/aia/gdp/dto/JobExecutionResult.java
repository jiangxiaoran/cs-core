package com.aia.gdp.dto;

import java.util.Date;
import java.util.Map;

/**
 * 作业执行结果
 */
public class JobExecutionResult {
    
    private String jobCode;
    private String batchNo;
    private String executorProc;
    private String executorAddress;
    private String status;
    private Date startTime;
    private Date endTime;
    private Integer duration;
    private String errorMessage;
    private boolean completed;
    private boolean success;
    private String message;
    private Map<String, Object> outputParameters;
    private Object returnValue;
    private long executionTime;
    private int affectedRows;
    
    public JobExecutionResult() {
        this.startTime = new Date();
    }
    
    public JobExecutionResult(String jobCode, String batchNo) {
        this();
        this.jobCode = jobCode;
        this.batchNo = batchNo;
        this.status = "pending";
    }
    
    // Getters and Setters
    public String getJobCode() {
        return jobCode;
    }
    
    public void setJobCode(String jobCode) {
        this.jobCode = jobCode;
    }
    
    public String getBatchNo() {
        return batchNo;
    }
    
    public void setBatchNo(String batchNo) {
        this.batchNo = batchNo;
    }
    
    public String getExecutorProc() {
        return executorProc;
    }
    
    public void setExecutorProc(String executorProc) {
        this.executorProc = executorProc;
    }
    
    public String getExecutorAddress() {
        return executorAddress;
    }
    
    public void setExecutorAddress(String executorAddress) {
        this.executorAddress = executorAddress;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public Date getStartTime() {
        return startTime;
    }
    
    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }
    
    public Date getEndTime() {
        return endTime;
    }
    
    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
    
    public Integer getDuration() {
        return duration;
    }
    
    public void setDuration(Integer duration) {
        this.duration = duration;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    public boolean isCompleted() {
        return completed;
    }
    
    public void setCompleted(boolean completed) {
        this.completed = completed;
        if (completed) {
            this.endTime = new Date();
            if (this.startTime != null) {
                // 将毫秒转换为秒
                long durationMs = this.endTime.getTime() - this.startTime.getTime();
                this.duration = (int) (durationMs / 1000);
            }
        }
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public void setSuccess(boolean success) {
        this.success = success;
        this.status = success ? "success" : "failed";
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    public Map<String, Object> getOutputParameters() {
        return outputParameters;
    }
    
    public void setOutputParameters(Map<String, Object> outputParameters) {
        this.outputParameters = outputParameters;
    }
    
    public Object getReturnValue() {
        return returnValue;
    }
    
    public void setReturnValue(Object returnValue) {
        this.returnValue = returnValue;
    }
    
    public long getExecutionTime() {
        return executionTime;
    }
    
    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }
    
    public int getAffectedRows() {
        return affectedRows;
    }
    
    public void setAffectedRows(int affectedRows) {
        this.affectedRows = affectedRows;
    }
    
    @Override
    public String toString() {
        return "JobExecutionResult{" +
                "jobCode='" + jobCode + '\'' +
                ", batchNo='" + batchNo + '\'' +
                ", executorProc='" + executorProc + '\'' +
                ", executorAddress='" + executorAddress + '\'' +
                ", status='" + status + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", duration=" + duration +
                ", errorMessage='" + errorMessage + '\'' +
                ", completed=" + completed +
                ", success=" + success +
                ", message='" + message + '\'' +
                ", outputParameters=" + outputParameters +
                ", returnValue=" + returnValue +
                ", executionTime=" + executionTime +
                ", affectedRows=" + affectedRows +
                '}';
    }
} 