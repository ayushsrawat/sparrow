package com.github.searchindex.spider;

import lombok.RequiredArgsConstructor;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RequiredArgsConstructor
public class SpiderManJob implements Job {

  private static final Logger logger = LoggerFactory.getLogger(SpiderManJob.class);

  private final SpiderMan spiderMan;

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    try {
      spiderMan.activatePowers(context);
    } catch (IOException e) {
      logger.error("Error executing SpiderManJob {}", context.getJobDetail(), e);
      throw new RuntimeException(e);
    }
  }

}
