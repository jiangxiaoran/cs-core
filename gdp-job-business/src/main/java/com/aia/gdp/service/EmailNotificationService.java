package com.aia.gdp.service;

import com.aia.gdp.model.JobDef;
import com.aia.gdp.model.JobExecutionLog;
import com.aia.gdp.handler.JobGroupDispatcherHandler.NotificationContent;

public interface EmailNotificationService {
    void sendFailureNotification(JobDef job, JobExecutionLog log);
    void sendGroupCompletionNotification(String jobGroup);
    void sendCompletionNotification(NotificationContent content);
} 