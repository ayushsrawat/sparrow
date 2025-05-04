package com.github.sparrow.util;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.TimeZone;

@Component
public class DateUtil {

  public long convertToLong(LocalDateTime localDateTime) {
    if (localDateTime == null) return 0L;
    return localDateTime.atZone(TimeZone.getDefault().toZoneId()).toInstant().toEpochMilli();
  }

  public LocalDateTime convertToLocalDateTime(long time) {
    if (time == 0L) return null;
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(time), TimeZone.getDefault().toZoneId());
  }

  @SuppressWarnings("unused")
  public Date convertToDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.toInstant(ZoneOffset.UTC));
  }

}
