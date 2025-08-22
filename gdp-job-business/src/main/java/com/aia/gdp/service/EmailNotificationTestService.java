package com.aia.gdp.service;

import com.aia.gdp.model.JobDef;
import com.aia.gdp.model.JobExecutionLog;
import com.aia.gdp.handler.JobGroupDispatcherHandler.NotificationContent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

/**
 * 邮件通知测试服务
 * 用于测试邮件通知功能
 */
@Service
public class EmailNotificationTestService {
    private static final Logger logger = LoggerFactory.getLogger(EmailNotificationTestService.class);
    
    @Autowired
    private EmailNotificationService emailNotificationService;
    
    /**
     * 测试失败通知
     */
    public void testFailureNotification() {
        logger.info("开始测试失败通知...");
        
        JobDef job = new JobDef();
        job.setJobCode("TEST_FAIL_001");
        job.setJobName("测试失败作业");
        job.setJobGroup("TEST_GROUP");
        job.setJobType("data_import");
        job.setNotifyEmail("test@company.com");
        
        JobExecutionLog log = new JobExecutionLog();
        log.setJobCode("TEST_FAIL_001");
        log.setStatus("failed");
        log.setErrorMessage("模拟作业执行失败");
        log.setStartTime(new Date());
        log.setEndTime(new Date());
        log.setDuration(30);
        
        emailNotificationService.sendFailureNotification(job, log);
        
        logger.info("失败通知测试完成");
    }
    
    /**
     * 测试分组完成通知
     */
    public void testGroupCompletionNotification() {
        logger.info("开始测试分组完成通知...");
        
        emailNotificationService.sendGroupCompletionNotification("TEST_GROUP");
        
        logger.info("分组完成通知测试完成");
    }
    
    /**
     * 测试完成通知
     */
    public void testCompletionNotification() {
        logger.info("开始测试完成通知...");
        
        NotificationContent content = new NotificationContent();
        content.setTotalGroups(2);
        content.setSuccessGroups(1);
        content.setFailureGroups(1);
        content.setTotalJobs(8);
        content.setSuccessJobs(6);
        content.setFailureJobs(2);
        content.setExecutionTime(new Date());
        
        emailNotificationService.sendCompletionNotification(content);
        
        logger.info("完成通知测试完成");
    }
    
    /**
     * 测试异步邮件发送
     */
    public void testAsyncEmailSending() {
        logger.info("开始测试异步邮件发送...");
        
        try {
            CompletableFuture<Boolean> future = 
                ((com.aia.gdp.service.impl.EmailNotificationServiceImpl) emailNotificationService)
                    .sendEmailAsync(
                        "测试邮件",
                        "这是一封测试邮件",
                        "test@company.com",
                        "TEST",
                        "TEST_001"
                    );
            
            Boolean result = future.get();
            logger.info("异步邮件发送测试结果: {}", result);
            
        } catch (Exception e) {
            logger.error("异步邮件发送测试失败", e);
        }
    }
    
    /**
     * 运行所有测试
     */
    public void runAllTests() {
        logger.info("=== 开始邮件通知功能测试 ===");
        
        try {
            testFailureNotification();
            Thread.sleep(1000);
            
            testGroupCompletionNotification();
            Thread.sleep(1000);
            
            testCompletionNotification();
            Thread.sleep(1000);
            
            testAsyncEmailSending();
            
            logger.info("=== 邮件通知功能测试完成 ===");
            
        } catch (Exception e) {
            logger.error("邮件通知功能测试失败", e);
        }
    }
} 