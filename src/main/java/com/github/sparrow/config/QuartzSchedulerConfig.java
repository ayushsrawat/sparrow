package com.github.sparrow.config;

import com.github.sparrow.spider.SpiderManJob;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class QuartzSchedulerConfig {

  private final Scheduler scheduler;

  @Value("${spider.cron.scheduler.expression}")
  private String cronSchedulerExpression;

  @PostConstruct
  public void scheduleSpiders() throws SchedulerException {
    JobDetail jobDetail = JobBuilder.newJob(SpiderManJob.class)
      .withIdentity("spiderJob", "spiders")
      .storeDurably()
      .build();

    Trigger trigger = TriggerBuilder.newTrigger()
      .withIdentity("spiderTrigger", "spiders")
      .forJob(jobDetail)
      .withSchedule(CronScheduleBuilder.cronSchedule(cronSchedulerExpression))
      .build();
    scheduler.scheduleJob(jobDetail, trigger);
  }

}
