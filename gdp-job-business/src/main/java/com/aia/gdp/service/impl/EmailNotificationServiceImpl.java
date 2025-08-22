package com.aia.gdp.service.impl;

import com.aia.gdp.model.JobDef;
import com.aia.gdp.model.JobExecutionLog;
import com.aia.gdp.model.EmailNotification;
import com.aia.gdp.mapper.EmailNotificationMapper;
import com.aia.gdp.service.EmailNotificationService;
import com.aia.gdp.handler.JobGroupDispatcherHandler.NotificationContent;
import com.aia.gdp.handler.JobGroupDispatcherHandler.GroupJobResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Async;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 邮件通知服务类
 *
 * @author andy
 * @date
 * @company
 */

@Service
public class EmailNotificationServiceImpl implements EmailNotificationService {
    private static final Logger logger = LoggerFactory.getLogger(EmailNotificationServiceImpl.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat FILE_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmmss");
    
    @Autowired
    private JavaMailSender mailSender;
    
    @Autowired
    private EmailNotificationMapper emailNotificationMapper;
    
    @Value("${spring.mail.username:system@company.com}")
    private String fromEmail;
    
    @Value("${spring.mail.properties.mail.smtp.auth:true}")
    private boolean smtpAuth;
    
    @Value("${jobs.notification.retry.count:3}")
    private int maxRetryCount;
    
    @Value("${jobs.notification.retry.delay:5000}")
    private long retryDelayMs;
    
    @Value("${jobs.notification.enabled:true}")
    private boolean notificationEnabled;
    
    @Value("${jobs.notification.admin.email:admin@company.com}")
    private String adminEmail;

    @Override
    public void sendFailureNotification(JobDef job, JobExecutionLog log) {
        if (!notificationEnabled) {
            logger.debug("邮件通知功能已禁用，跳过失败通知");
            return;
        }
        
        try {
            // 构建邮件内容
            String subject = buildFailureSubject(job);
            String content = buildFailureContent(job, log);
            String recipient = determineRecipient(job);
            
            // 异步发送邮件
            sendEmailAsync(subject, content, recipient, "FAILURE", job.getJobCode());
            
            logger.info("作业失败通知已发送: {} -> {}", job.getJobCode(), recipient);
            
        } catch (Exception e) {
            logger.error("发送作业失败通知异常: {}", job.getJobCode(), e);
            // 记录到数据库
            saveNotificationRecord(job.getJobCode(), "FAILURE", "发送失败: " + e.getMessage());
        }
    }

    @Override
    public void sendGroupCompletionNotification(String jobGroup) {
        if (!notificationEnabled) {
            logger.debug("邮件通知功能已禁用，跳过分组完成通知");
            return;
        }
        
        try {
            String subject = buildGroupCompletionSubject(jobGroup);
            String content = buildGroupCompletionContent(jobGroup);
            
            // 发送给管理员
            sendEmailAsync(subject, content, adminEmail, "GROUP_COMPLETION", jobGroup);
            
            logger.info("分组完成通知已发送: {}", jobGroup);
            
        } catch (Exception e) {
            logger.error("发送分组完成通知异常: {}", jobGroup, e);
            saveNotificationRecord(jobGroup, "GROUP_COMPLETION", "发送失败: " + e.getMessage());
        }
    }

    @Override
    public void sendCompletionNotification(NotificationContent content) {
        if (!notificationEnabled) {
            logger.debug("邮件通知功能已禁用，跳过完成通知");
            return;
        }
        
        try {
            String subject = buildCompletionSubject(content);
            String htmlContent = buildCompletionHtmlContent(content);
            
            // 发送给管理员
            sendEmailAsync(subject, htmlContent, adminEmail, "COMPLETION", "BATCH_COMPLETION", true);
            
            logger.info("作业分组调度完成通知已发送");
            
        } catch (Exception e) {
            logger.error("发送完成通知异常", e);
            saveNotificationRecord("BATCH_COMPLETION", "COMPLETION", "发送失败: " + e.getMessage());
        }
    }
    
    /**
     * 异步发送邮件
     */
    @Async("emailTaskExecutor")
    public CompletableFuture<Boolean> sendEmailAsync(String subject, String content, 
                                                   String recipient, String type, String jobCode) {
        return sendEmailAsync(subject, content, recipient, type, jobCode, false);
    }
    
    /**
     * 异步发送HTML邮件
     */
    @Async("emailTaskExecutor")
    public CompletableFuture<Boolean> sendEmailAsync(String subject, String content, 
                                                   String recipient, String type, String jobCode, boolean isHtml) {
        return CompletableFuture.supplyAsync(() -> {
            int retryCount = 0;
            Exception lastException = null;
            
            while (retryCount <= maxRetryCount) {
                try {
                    MimeMessage message = mailSender.createMimeMessage();
                    MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");
                    
                    helper.setFrom(fromEmail);
                    helper.setTo(recipient);
                    helper.setSubject(subject);
                    helper.setText(content, isHtml);
                    
                    // 添加邮件头信息
                    message.setHeader("X-Job-Type", type);
                    message.setHeader("X-Job-Code", jobCode);
                    message.setHeader("X-Send-Time", DATE_FORMAT.format(new Date()));
                    
                    mailSender.send(message);
                    
                    // 记录成功发送
                    saveNotificationRecord(jobCode, type, "发送成功");
                    
                    logger.info("邮件发送成功: {} -> {} (重试次数: {})", subject, recipient, retryCount);
                    return true;
                    
                } catch (MessagingException e) {
                    lastException = e;
                    retryCount++;
                    
                    if (retryCount <= maxRetryCount) {
                        logger.warn("邮件发送失败，准备重试 ({}/{}): {} -> {}, 错误: {}", 
                                   retryCount, maxRetryCount, subject, recipient, e.getMessage());
                        
                        try {
                            TimeUnit.MILLISECONDS.sleep(retryDelayMs * retryCount);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
            
            // 所有重试都失败了
            String errorMsg = String.format("邮件发送失败，已重试%d次: %s", maxRetryCount, 
                                          lastException != null ? lastException.getMessage() : "未知错误");
            saveNotificationRecord(jobCode, type, errorMsg);
            logger.error("邮件发送最终失败: {} -> {}", subject, recipient, lastException);
            
            return false;
        });
    }
    
    /**
     * 构建失败通知主题
     */
    private String buildFailureSubject(JobDef job) {
        return String.format("[作业失败通知] %s (%s)", job.getJobName(), job.getJobCode());
    }
    
    /**
     * 构建失败通知内容
     */
    private String buildFailureContent(JobDef job, JobExecutionLog log) {
        StringBuilder content = new StringBuilder();
        content.append("=== 作业执行失败通知 ===\n\n");
        content.append("作业名称: ").append(job.getJobName()).append("\n");
        content.append("作业代码: ").append(job.getJobCode()).append("\n");
        content.append("作业分组: ").append(job.getJobGroup()).append("\n");
        content.append("作业类型: ").append(job.getJobType()).append("\n");
        content.append("失败时间: ").append(DATE_FORMAT.format(log.getStartTime())).append("\n");
        content.append("执行时长: ").append(log.getDuration()).append(" 秒\n");
        content.append("错误信息: ").append(log.getErrorMessage()).append("\n\n");
        content.append("请及时检查作业配置和执行环境。\n");
        content.append("---\n");
        content.append("此邮件由系统自动发送，请勿直接回复。");
        
        return content.toString();
    }
    
    /**
     * 构建分组完成通知主题
     */
    private String buildGroupCompletionSubject(String jobGroup) {
        return String.format("[分组完成通知] %s", jobGroup);
    }
    
    /**
     * 构建分组完成通知内容
     */
    private String buildGroupCompletionContent(String jobGroup) {
        StringBuilder content = new StringBuilder();
        content.append("=== 作业分组执行完成通知 ===\n\n");
        content.append("分组名称: ").append(jobGroup).append("\n");
        content.append("完成时间: ").append(DATE_FORMAT.format(new Date())).append("\n\n");
        content.append("分组内所有作业已执行完成。\n");
        content.append("---\n");
        content.append("此邮件由系统自动发送，请勿直接回复。");
        
        return content.toString();
    }
    
    /**
     * 构建完成通知主题
     */
    private String buildCompletionSubject(NotificationContent content) {
        String status = content.getFailureJobs() > 0 ? "部分成功" : "全部成功";
        return String.format("[作业调度完成通知] %s (成功:%d, 失败:%d)", 
                           status, content.getSuccessJobs(), content.getFailureJobs());
    }
    
    /**
     * 构建完成通知HTML内容
     */
    private String buildCompletionHtmlContent(NotificationContent content) {
        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>");
        html.append("<html><head>");
        html.append("<meta charset=\"UTF-8\">");
        html.append("<title>作业调度完成通知</title>");
        html.append("<style>");
        html.append("body { font-family: Arial, sans-serif; margin: 20px; }");
        html.append(".header { background-color: #f8f9fa; padding: 15px; border-radius: 5px; }");
        html.append(".success { color: #28a745; }");
        html.append(".failure { color: #dc3545; }");
        html.append(".warning { color: #ffc107; }");
        html.append(".table { width: 100%; border-collapse: collapse; margin: 15px 0; }");
        html.append(".table th, .table td { border: 1px solid #ddd; padding: 8px; text-align: left; }");
        html.append(".table th { background-color: #f2f2f2; }");
        html.append(".footer { margin-top: 20px; padding: 10px; background-color: #f8f9fa; border-radius: 5px; font-size: 12px; }");
        html.append("</style>");
        html.append("</head><body>");
        
        // 标题
        html.append("<div class=\"header\">");
        html.append("<h2>作业分组调度执行完成通知</h2>");
        html.append("<p>执行时间: ").append(DATE_FORMAT.format(content.getExecutionTime())).append("</p>");
        html.append("</div>");
        
        // 统计信息
        html.append("<h3>执行统计</h3>");
        html.append("<table class=\"table\">");
        html.append("<tr><th>指标</th><th>数量</th></tr>");
        html.append("<tr><td>总分组数</td><td>").append(content.getTotalGroups()).append("</td></tr>");
        html.append("<tr><td>成功分组数</td><td class=\"success\">").append(content.getSuccessGroups()).append("</td></tr>");
        html.append("<tr><td>失败分组数</td><td class=\"failure\">").append(content.getFailureGroups()).append("</td></tr>");
        html.append("<tr><td>总作业数</td><td>").append(content.getTotalJobs()).append("</td></tr>");
        html.append("<tr><td>成功作业数</td><td class=\"success\">").append(content.getSuccessJobs()).append("</td></tr>");
        html.append("<tr><td>失败作业数</td><td class=\"failure\">").append(content.getFailureJobs()).append("</td></tr>");
        html.append("</table>");
        
        // 分组详情
        if (content.getGroupResults() != null && !content.getGroupResults().isEmpty()) {
            html.append("<h3>分组执行详情</h3>");
            html.append("<table class=\"table\">");
            html.append("<tr><th>分组</th><th>总作业数</th><th>成功数</th><th>失败数</th><th>状态</th></tr>");
            
            for (Map.Entry<String, GroupJobResult> entry : content.getGroupResults().entrySet()) {
                String groupName = entry.getKey();
                GroupJobResult result = entry.getValue();
                String status = result.getErrorMessage() != null ? "失败" : "成功";
                String statusClass = result.getErrorMessage() != null ? "failure" : "success";
                
                html.append("<tr>");
                html.append("<td>").append(groupName).append("</td>");
                html.append("<td>").append(result.getTotalJobs()).append("</td>");
                html.append("<td class=\"success\">").append(result.getSuccessCount()).append("</td>");
                html.append("<td class=\"failure\">").append(result.getFailureCount()).append("</td>");
                html.append("<td class=\"").append(statusClass).append("\">").append(status).append("</td>");
                html.append("</tr>");
            }
            html.append("</table>");
            
            // 失败作业详情
            boolean hasFailedJobs = content.getGroupResults().values().stream()
                    .anyMatch(result -> !result.getFailedJobs().isEmpty());
            
            if (hasFailedJobs) {
                html.append("<h3>失败作业详情</h3>");
                html.append("<table class=\"table\">");
                html.append("<tr><th>分组</th><th>失败作业</th><th>错误信息</th></tr>");
                
                for (Map.Entry<String, GroupJobResult> entry : content.getGroupResults().entrySet()) {
                    String groupName = entry.getKey();
                    GroupJobResult result = entry.getValue();
                    
                    if (!result.getFailedJobs().isEmpty()) {
                        for (String failedJob : result.getFailedJobs()) {
                            String[] parts = failedJob.split(":", 2);
                            String jobCode = parts[0];
                            String errorMsg = parts.length > 1 ? parts[1] : "未知错误";
                            
                            html.append("<tr>");
                            html.append("<td>").append(groupName).append("</td>");
                            html.append("<td class=\"failure\">").append(jobCode).append("</td>");
                            html.append("<td class=\"failure\">").append(errorMsg).append("</td>");
                            html.append("</tr>");
                        }
                    }
                }
                html.append("</table>");
            }
        }
        
        // 页脚
        html.append("<div class=\"footer\">");
        html.append("<p>此邮件由系统自动发送，请勿直接回复。</p>");
        html.append("<p>如有问题，请联系系统管理员。</p>");
        html.append("</div>");
        
        html.append("</body></html>");
        
        return html.toString();
    }
    
    /**
     * 确定收件人
     */
    private String determineRecipient(JobDef job) {
        // 优先使用作业配置的通知邮箱
        if (job.getNotifyEmail() != null && !job.getNotifyEmail().trim().isEmpty()) {
            return job.getNotifyEmail().trim();
        }
        
        // 使用默认管理员邮箱
        return adminEmail;
    }
    
    /**
     * 保存通知记录到数据库
     */
    private void saveNotificationRecord(String jobCode, String type, String status) {
        try {
            // 生成唯一的通知记录ID（使用时间戳）
            String uniqueJobCode = jobCode + "_" + System.currentTimeMillis();
            
            EmailNotification notification = new EmailNotification();
            notification.setJobCode(uniqueJobCode);
            notification.setRecipientEmail(adminEmail);
            notification.setSubject("系统通知");
            notification.setBody("通知类型: " + type + ", 状态: " + status);
            notification.setStatus(status.contains("成功") ? "SENT" : "FAILED");
            notification.setSendTime(new Date());
            notification.setCreatedAt(new Date());
            notification.setUpdatedAt(new Date());
            notification.setType(type);
            notification.setFailReason(status.contains("失败") ? status : null);
            notification.setRetryCount(0);
            notification.setIsSystem(true);
            
            emailNotificationMapper.insert(notification);
            logger.info("保存通知记录: {}", uniqueJobCode);
            
        } catch (Exception e) {
            logger.error("保存通知记录失败: {}", jobCode, e);
        }
    }
    
    /**
     * 获取通知历史记录
     */
    public List<EmailNotification> getNotificationHistory(String jobCode) {
        return emailNotificationMapper.selectByJobCode(jobCode);
    }
    
    /**
     * 重试失败的通知
     */
    public void retryFailedNotifications() {
        try {
            // 查找失败的通知记录
            List<EmailNotification> failedNotifications = emailNotificationMapper.selectAll();
            
            for (EmailNotification notification : failedNotifications) {
                if ("FAILED".equals(notification.getStatus()) && 
                    notification.getRetryCount() < maxRetryCount) {
                    
                    // 重试发送
                    notification.setRetryCount(notification.getRetryCount() + 1);
                    emailNotificationMapper.update(notification);
                    
                    // 异步重试发送
                    sendEmailAsync(
                        notification.getSubject(),
                        notification.getBody(),
                        notification.getRecipientEmail(),
                        notification.getType(),
                        notification.getJobCode()
                    );
                }
            }
            
        } catch (Exception e) {
            logger.error("重试失败通知异常", e);
        }
    }
} 