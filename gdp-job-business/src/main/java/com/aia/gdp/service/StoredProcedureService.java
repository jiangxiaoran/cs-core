package com.aia.gdp.service;

import java.util.Map;

/**
 * 存储过程执行服务接口
 * 提供专业的存储过程调用功能
 */
public interface StoredProcedureService {
    
    /**
     * 执行存储过程
     * 
     * @param procedureName 存储过程名称
     * @param parameters 输入参数
     * @return 执行结果
     * @throws Exception 执行异常
     */
    StoredProcedureResult executeProcedure(String procedureName, Map<String, Object> parameters) throws Exception;
    
    /**
     * 执行存储过程（无参数）
     * 
     * @param procedureName 存储过程名称
     * @return 执行结果
     * @throws Exception 执行异常
     */
    StoredProcedureResult executeProcedure(String procedureName) throws Exception;
    
    /**
     * 检查存储过程是否存在
     * 
     * @param procedureName 存储过程名称
     * @return 是否存在
     */
    boolean procedureExists(String procedureName);
    
    /**
     * 获取存储过程参数信息
     * 
     * @param procedureName 存储过程名称
     * @return 参数信息
     */
    Map<String, String> getProcedureParameters(String procedureName);
    
    /**
     * 存储过程执行结果
     */
    class StoredProcedureResult {
        private boolean success;
        private String message;
        private Map<String, Object> outputParameters;
        private Object returnValue;
        private long executionTime;
        private int affectedRows;
        
        public StoredProcedureResult() {}
        
        public StoredProcedureResult(boolean success, String message) {
            this.success = success;
            this.message = message;
        }
        
        // Getters and Setters
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        
        public Map<String, Object> getOutputParameters() { return outputParameters; }
        public void setOutputParameters(Map<String, Object> outputParameters) { this.outputParameters = outputParameters; }
        
        public Object getReturnValue() { return returnValue; }
        public void setReturnValue(Object returnValue) { this.returnValue = returnValue; }
        
        public long getExecutionTime() { return executionTime; }
        public void setExecutionTime(long executionTime) { this.executionTime = executionTime; }
        
        public int getAffectedRows() { return affectedRows; }
        public void setAffectedRows(int affectedRows) { this.affectedRows = affectedRows; }
        
        @Override
        public String toString() {
            return "StoredProcedureResult{" +
                    "success=" + success +
                    ", message='" + message + '\'' +
                    ", outputParameters=" + outputParameters +
                    ", returnValue=" + returnValue +
                    ", executionTime=" + executionTime +
                    ", affectedRows=" + affectedRows +
                    '}';
        }
    }
} 