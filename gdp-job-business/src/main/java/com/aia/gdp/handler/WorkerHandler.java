package com.aia.gdp.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.aia.gdp.model.JobDef;
import com.aia.gdp.model.JobExecutionLog;
import com.aia.gdp.dto.JobExecutionResult;
import com.aia.gdp.service.StoredProcedureService;
import com.aia.gdp.service.JobControlService;

import com.aia.gdp.common.DateUtils;
import com.aia.gdp.common.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.aia.gdp.common.CacheKeyUtils;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Map;
import java.util.HashMap;
import java.net.InetAddress;
/**
 * 作业任务处理 可扩展多种处理方式 暂时先实现存储过程执行
 * 可扩展java class类 python 脚本  命令行脚本 javascript脚本 等
 * 此处冗余放置转换函数，不希望把数据库的相关信息暴露给作业系统
 * @author andy
 * @date 2025-07-28
 * @company
 */
@Component
public class WorkerHandler {
    private static final Logger logger = LoggerFactory.getLogger(WorkerHandler.class);

    @Autowired
    private StoredProcedureService storedProcedureService;
    
    @Autowired
    private JobControlService jobControlService;

    public JobExecutionResult executeJob(JobDef job, String batchNo) {
        JobExecutionResult result = new JobExecutionResult(job.getJobCode(), batchNo);
        result.setExecutorProc(job.getJobType());
        result.setExecutorAddress(getExecutorAddress());

        //String jobKey = CacheKeyUtils.generateJobKey(job.getJobCode(), batchNo);
        //String groupKey = CacheKeyUtils.generateGroupKey(job.getJobGroup(), batchNo);

        
        try {
            logger.info("开始执行作业:{} {} ({})", batchNo, job.getJobName(), job.getJobCode());
            // 执行前检查组停止请求
            if (jobControlService.isGroupStopped(job.getJobGroup(),batchNo)) {
                logger.info("作业组 {} 收到停止请求，停止执行", job.getJobGroup());
                result.setCompleted(false);
                result.setSuccess(false);
                result.setErrorMessage("作业被请求停止");
                return result;
            }
            // 执行前检查作业停止请求

            if (jobControlService.isJobStopped(job.getJobCode(),batchNo)) {
                logger.info("作业 {} 收到停止请求，中断执行", job.getJobCode());
                result.setCompleted(false);
                result.setSuccess(false);
                result.setErrorMessage("作业被请求停止");
                return result;
            }

            if (jobControlService.isGroupCancelled(job.getJobGroup(),batchNo)) {
                logger.info("作业 {} 收到取消请求，取消执行", job.getJobCode());
                result.setCompleted(false);
                result.setSuccess(false);
                result.setErrorMessage("作业被请求取消");
                return result;
            }

            if (jobControlService.isJobCancelled(job.getJobCode(),batchNo)) {
                logger.info("作业 {} 收到取消请求，取消执行", job.getJobCode());
                result.setCompleted(false);
                result.setSuccess(false);
                result.setErrorMessage("作业被请求取消");
                return result;
            }
            // 🎯 执行前将作业状态改为 RUNNING
            jobControlService.resetJobStatus(job.getJobCode(),batchNo);
            logger.info("作业 {} 状态设置为 RUNNING，开始执行", job.getJobCode());

            // 执行单个作业
            logger.info("执行作业: {} ({})", job.getJobName(), job.getJobCode());

            // 不同类型的作业执行
            if (job.getJobType() != null) {
                switch (job.getJobType().toLowerCase()) {
                    case "data_import":
                        simulateDataImportJob(job);
                        break;
                    case "report_generation":
                        simulateReportGenerationJob(job);
                        break;
                    case "data_cleanup":
                        simulateDataCleanupJob(job);
                        break;
                    case "notification":
                        simulateNotificationJob(job);
                        break;
                    case "stored_procedure":
                        executeStoredProcedureJob(job);
                        break;
                    default:
                        simulateGenericJob(job);
                        break;
                }
            } else {
                simulateGenericJob(job);
            }
            
            // 模拟随机失败（用于测试）
            /*
            if (shouldSimulateFailure(job)) {
                throw new RuntimeException("模拟作业执行失败: " + job.getJobName());
            }
            */
            
            result.setCompleted(true);
            result.setSuccess(true);
            logger.info("作业执行成功: {} ({})", job.getJobName(), job.getJobCode());
            
        } catch (Exception e) {
            result.setCompleted(false);
            result.setErrorMessage(e.getMessage());
            logger.error("作业执行失败: {} ({}), 错误: {}", job.getJobName(), job.getJobCode(), e.getMessage());
        }
        
        return result;
    }
    
    /**
     * 执行作业（向后兼容方法）
     */
    public JobExecutionLog executeJob(JobDef job) {
        JobExecutionResult result = executeJob(job, Utils.generateBatchNo());
        // 转换为JobExecutionLog以保持向后兼容
        JobExecutionLog log = new JobExecutionLog();
        log.setJobCode(result.getJobCode());
        log.setBatchNo(result.getBatchNo());
        log.setStatus(result.getStatus());
        log.setStartTime(result.getStartTime());
        log.setEndTime(result.getEndTime());
        log.setDuration(result.getDuration());
        log.setErrorMessage(result.getErrorMessage());
        log.setExecutorProc(result.getExecutorProc());
        log.setExecutorAddress(result.getExecutorAddress());
        return log;
    }
    
    /**
     * 获取执行器地址
     */
    private String getExecutorAddress() {
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            return localHost.getHostAddress() + ":8081";
        } catch (Exception e) {
            logger.warn("获取执行器地址失败", e);
            return "unknown:8081";
        }
    }
    
    /**
     * 执行存储过程作业
     * 根据作业名称和参数调用相应的存储过程
     */
    private void executeStoredProcedureJob(JobDef job) throws Exception {
        logger.info("开始执行存储过程作业: {} ({})", job.getJobName(), job.getJobCode());
        
        // 根据作业名称或代码确定要执行的存储过程
        String procedureName = determineProcedureName(job);
        // 根据配置参数构建调用参数，并且参数有特殊处理的会单独处理
        Map<String, Object> parameters = buildProcedureParameters(job);
        
        // 执行存储过程
        StoredProcedureService.StoredProcedureResult result = 
            storedProcedureService.executeProcedure(procedureName, parameters);
        
        if (!result.isSuccess()) {
            throw new RuntimeException("存储过程执行失败: " + result.getMessage());
        }
        
        logger.info("存储过程执行成功: {}, 耗时: {}ms, 影响行数: {}", 
                   procedureName, result.getExecutionTime(), result.getAffectedRows());
        
        // 记录输出参数（如果有）
        if (result.getOutputParameters() != null && !result.getOutputParameters().isEmpty()) {
            logger.info("存储过程输出参数: {}", result.getOutputParameters());
        }
    }
    
    /**
     * 根据作业信息确定要执行的存储过程名称
     */
    private String determineProcedureName(JobDef job) {
        // 如果作业定义了存储过程名称，直接使用
        if (job.getProcName() != null && !job.getProcName().trim().isEmpty()) {
            return job.getProcName();
        }
        
        // 根据作业代码或名称推断存储过程名称
        String jobCode = job.getJobCode() != null ? job.getJobCode().toLowerCase() : "";
        String jobName = job.getJobName() != null ? job.getJobName().toLowerCase() : "";
        
        // 特殊处理已知的存储过程
        if (jobCode.equals("fr_pricing_ASOReportBL") || jobName.contains("ASOReportBL")) {
            return "fr_pricing_ASOReportBL";
        }else if (jobCode.contains("fr_pricing_ActIBNRReportBL") || jobName.contains("fr_pricing_ActIBNRReportBL")) {
            return "fr_pricing_ActIBNRReportBL";
        }else if (jobCode.contains("fr_pricing_Batch_GTA_REPORT_TASK") || jobName.contains("fr_pricing_Batch_GTA_REPORT_TASK")) {
            return "fr_pricing_Batch_GTA_REPORT_TASK";
        }else if (jobCode.contains("fr_pricing_CASUPRAnnualizedPremiumReportBL") || jobName.contains("fr_pricing_CASUPRAnnualizedPremiumReportBL")) {
            return "fr_pricing_CASUPRAnnualizedPremiumReportBL";
        }else if (jobCode.contains("fr_pricing_GVSCnolmmReportBL") || jobName.contains("fr_pricing_GVSCnolmmReportBL")) {
            return "fr_pricing_GVSCnolmmReportBL";
        }else if (jobCode.contains("fr_pricing_GVSCnolmmReportDateBL") || jobName.contains("fr_pricing_GVSCnolmmReportDateBL")) {
            return "fr_pricing_GVSCnolmmReportDateBL";
        }else if (jobCode.contains("fr_pricing_GVS_CNOLMM_REPORT") || jobName.contains("fr_pricing_GVS_CNOLMM_REPORT")) {
            return "fr_pricing_GVS_CNOLMM_REPORT";
        }else if (jobCode.contains("fr_pricing_IFRSCoolOffGrpContDataCreateBL") || jobName.contains("fr_pricing_IFRSCoolOffGrpContDataCreateBL")) {
            return "fr_pricing_IFRSCoolOffGrpContDataCreateBL";
        }else if (jobCode.contains("fr_pricing_IFRS_EB_01") || jobName.contains("fr_pricing_IFRS_EB_01")) {
            return "fr_pricing_IFRS_EB_01";
        }else if (jobCode.contains("fr_pricing_IFRS_GVS_07") || jobName.contains("fr_pricing_IFRS_GVS_07")) {
            return "fr_pricing_IFRS_GVS_07";
        }else if (jobCode.contains("fr_pricing_UPRMonthDataReportBL") || jobName.contains("fr_pricing_UPRMonthDataReportBL")) {
            return "fr_pricing_UPRMonthDataReportBL";
        }else if (jobCode.contains("pfr_pricing_GVSCompassnbReportTwoBL") || jobName.contains("pfr_pricing_GVSCompassnbReportTwoBL")) {
            return "pfr_pricing_GVSCompassnbReportTwoBL";
        }
        else if (jobCode.contains("pfr_pricing_IFRS_LIC_01") || jobName.contains("pfr_pricing_IFRS_LIC_01")) {
            return "pfr_pricing_IFRS_LIC_01";
        }else if (jobCode.contains("pfr_pricing_IFRS_UDP_POL_INFORCE") || jobName.contains("pfr_pricing_IFRS_UDP_POL_INFORCE")) {
            return "pfr_pricing_IFRS_UDP_POL_INFORCE";
        }
        else if (jobCode.contains("cleanup") || jobName.contains("清理")) {
            return "sp_cleanup_old_data";
        } else if (jobCode.contains("statistics") || jobName.contains("统计")) {
            return "sp_generate_daily_statistics";
        } else if (jobCode.contains("sync") || jobName.contains("同步")) {
            return "sp_sync_external_data";
        } else if (jobCode.contains("report") || jobName.contains("报表")) {
            return "sp_generate_report";
        } else if (jobCode.contains("validate") || jobName.contains("验证")) {
            return "sp_validate_data_integrity";
        } else if (jobCode.contains("archive") || jobName.contains("归档")) {
            return "sp_archive_historical_data";
        } else if (jobCode.contains("maintenance") || jobName.contains("维护")) {
            return "sp_system_maintenance";
        } else if (jobCode.contains("user") || jobName.contains("用户")) {
            return "sp_sync_user_data";
        } else if (jobCode.contains("order") || jobName.contains("订单")) {
            return "sp_process_order_data";
        } else if (jobCode.contains("financial") || jobName.contains("财务")) {
            return "sp_generate_financial_summary";
        } else {
            // 默认存储过程
            return "sp_generic_batch_process";
        }
    }
    
    /**
     * 构建存储过程参数
     */
    private Map<String, Object> buildProcedureParameters(JobDef job) {
        Map<String, Object> parameters = new HashMap<>();

        // 首先尝试从 job_params 中读取参数
        if (job.getJobParams() != null && !job.getJobParams().trim().isEmpty()) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode params = mapper.readTree(job.getJobParams());
                params.fields().forEachRemaining(entry -> {
                    String value = entry.getValue().asText();
                    // 处理日期占位符
                    String processedValue = DateUtils.processDatePlaceholders(value);
                    parameters.put(entry.getKey(), processedValue);
                });
                logger.info("从作业参数中读取到 {} 个参数", parameters.size());
            } catch (Exception e) {
                logger.warn("解析作业参数失败: {}", e.getMessage());
            }
        }
        
        // 根据作业类型添加特定参数
        //String jobType = job.getJobType() != null ? job.getJobType().toLowerCase() : "";
        //String jobCode = job.getJobCode() != null ? job.getJobCode().toLowerCase() : "";
        //String procedureName = determineProcedureName(job);
        

        
        return parameters;
    }
    



    
    /**
     * 从作业信息中提取表名
     */
    private String extractTableName(String jobCode, String jobName) {
        if (jobCode.contains("user")) return "user_data";
        if (jobCode.contains("order")) return "order_data";
        if (jobCode.contains("product")) return "product_data";
        if (jobCode.contains("log")) return "system_logs";
        if (jobName != null && jobName.contains("用户")) return "user_data";
        if (jobName != null && jobName.contains("订单")) return "order_data";
        if (jobName != null && jobName.contains("产品")) return "product_data";
        if (jobName != null && jobName.contains("日志")) return "system_logs";
        return "default_table";
    }
    
    /**
     * 从作业信息中提取报表类型
     */
    private String extractReportType(String jobCode, String jobName) {
        if (jobCode.contains("daily") || (jobName != null && jobName.contains("日报"))) return "DAILY";
        if (jobCode.contains("weekly") || (jobName != null && jobName.contains("周报"))) return "WEEKLY";
        if (jobCode.contains("monthly") || (jobName != null && jobName.contains("月报"))) return "MONTHLY";
        if (jobCode.contains("yearly") || (jobName != null && jobName.contains("年报"))) return "YEARLY";
        return "DAILY";
    }
    
    /**
     * 从作业信息中提取源系统
     */
    private String extractSourceSystem(String jobCode, String jobName) {
        if (jobCode.contains("erp") || (jobName != null && jobName.contains("ERP"))) return "ERP";
        if (jobCode.contains("crm") || (jobName != null && jobName.contains("CRM"))) return "CRM";
        if (jobCode.contains("oms") || (jobName != null && jobName.contains("OMS"))) return "OMS";
        return "EXTERNAL";
    }
    
    /**
     * 从作业信息中提取目标表
     */
    private String extractTargetTable(String jobCode, String jobName) {
        return extractTableName(jobCode, jobName);
    }
    
    /**
     * 从作业信息中提取维护类型
     */
    private String extractMaintenanceType(String jobCode, String jobName) {
        if (jobCode.contains("backup") || (jobName != null && jobName.contains("备份"))) return "BACKUP";
        if (jobCode.contains("optimize") || (jobName != null && jobName.contains("优化"))) return "OPTIMIZE";
        if (jobCode.contains("cleanup") || (jobName != null && jobName.contains("清理"))) return "CLEANUP";
        return "GENERAL";
    }
    
    /**
     * 从作业信息中提取汇总类型
     */
    private String extractSummaryType(String jobCode, String jobName) {
        if (jobCode.contains("revenue") || (jobName != null && jobName.contains("收入"))) return "REVENUE";
        if (jobCode.contains("cost") || (jobName != null && jobName.contains("成本"))) return "COST";
        if (jobCode.contains("profit") || (jobName != null && jobName.contains("利润"))) return "PROFIT";
        return "GENERAL";
    }
    
    /**
     * 模拟数据导入作业
     */
    private void simulateDataImportJob(JobDef job) throws InterruptedException {
        // 模拟数据导入过程
        Thread.sleep(30000 + ThreadLocalRandom.current().nextInt(3000)); // 2-5秒
        
        // 模拟处理数据量
        int recordCount = 1000 + ThreadLocalRandom.current().nextInt(5000);
        logger.info("数据导入作业处理了 {} 条记录", recordCount);
    }
    
    /**
     * 模拟报表生成作业
     */
    private void simulateReportGenerationJob(JobDef job) throws InterruptedException {
        // 模拟报表生成过程
        Thread.sleep(32000 + ThreadLocalRandom.current().nextInt(4000)); // 3-7秒
        
        // 模拟生成报表
        String reportType = "日报";
        if (job.getJobName() != null && job.getJobName().contains("月")) {
            reportType = "月报";
        }
        logger.info("生成{}: {}", reportType, job.getJobName());
    }
    
    /**
     * 模拟数据清理作业
     */
    private void simulateDataCleanupJob(JobDef job) throws InterruptedException {
        // 模拟数据清理过程
        Thread.sleep(30000 + ThreadLocalRandom.current().nextInt(2000)); // 1-3秒
        
        // 模拟清理结果
        int cleanedRecords = 100 + ThreadLocalRandom.current().nextInt(500);
        logger.info("数据清理作业清理了 {} 条过期记录", cleanedRecords);
    }
    
    /**
     * 模拟通知作业
     */
    private void simulateNotificationJob(JobDef job) throws InterruptedException {
        // 模拟通知发送过程
        Thread.sleep(500 + ThreadLocalRandom.current().nextInt(1000)); // 0.5-1.5秒
        
        // 模拟发送通知
        String notificationType = "邮件";
        if (job.getJobName() != null && job.getJobName().contains("短信")) {
            notificationType = "短信";
        }
        logger.info("发送{}通知: {}", notificationType, job.getJobName());
    }
    
    /**
     * 模拟通用作业
     */
    private void simulateGenericJob(JobDef job) throws InterruptedException {
        // 模拟通用作业执行
        Thread.sleep(30000 + ThreadLocalRandom.current().nextInt(2000)); // 1-3秒
        
        logger.info("执行通用作业: {}", job.getJobName());
    }
    
    /**
     * 模拟随机失败（用于测试）
     * 在实际生产环境中，这个方法应该被移除或设置为极低的失败概率
     */
    private boolean shouldSimulateFailure(JobDef job) {
        // 只对明确标记为测试失败的作业才失败
        if (job.getJobCode() != null && job.getJobCode().contains("TEST_FAIL")) {
            return true; // 强制失败的测试作业
        }
        
        // 对于正常作业，不设置随机失败
        // 在生产环境中，应该完全移除随机失败逻辑
        return false; // 正常作业不会随机失败
    }
} 