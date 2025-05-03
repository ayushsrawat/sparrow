package com.github.searchindex.config;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

@Configuration
public class QuartzConfig {

  /**
   * A {@link org.quartz.Job} requires a public no-argument constructor to be instantiated by Quartz.
   * However, by using {@link org.springframework.scheduling.quartz.SpringBeanJobFactory},
   * Spring's dependency injection is enabled, allowing Spring-managed beans to be injected into jobs.
   */
  @Bean
  public Scheduler scheduler(SpringBeanJobFactory springBeanJobFactory) throws SchedulerException {
    Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
    scheduler.setJobFactory(springBeanJobFactory);
    scheduler.start();
    return scheduler;
  }

  @Bean
  public SpringBeanJobFactory springBeanJobFactory(ApplicationContext applicationContext) {
    SpringBeanJobFactory springBeanJobFactory = new SpringBeanJobFactory();
    springBeanJobFactory.setApplicationContext(applicationContext);
    return springBeanJobFactory;
  }

}
