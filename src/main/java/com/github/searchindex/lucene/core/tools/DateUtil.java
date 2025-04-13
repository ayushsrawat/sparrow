package com.github.searchindex.lucene.core.tools;

import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

@Component
public class DateUtil {

  public long convertToLong(LocalDateTime localDateTime) {
    return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  public Date convertToDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.toInstant(ZoneOffset.UTC));
  }

}
