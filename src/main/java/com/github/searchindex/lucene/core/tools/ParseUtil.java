package com.github.searchindex.lucene.core.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

@Component
public class ParseUtil {

  private static final Logger logger = LoggerFactory.getLogger(ParseUtil.class);

  public Long parseLong(String value) {
    try {
      return value == null || value.isBlank() ? 0L : Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return 0L;
    }
  }

  public Integer parseInt(String value) {
    try {
      return value == null || value.isBlank() ? 0 : Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  //Mon Apr 06 22:19:49 PDT 2009
  public LocalDateTime parseDateV1(String date) {
    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
      ZonedDateTime zdt = ZonedDateTime.parse(date, formatter);
      return zdt.toLocalDateTime();
    } catch (DateTimeException e) {
      logger.warn("Failed to parse V1 date: {}", date);
      return null;
    }
  }

  //2024-05-29T06:30:33.000Z
  public LocalDateTime parseDateV2(String date) {
    try {
      String cleanDate = date.trim().replace("\"", "");
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
      OffsetDateTime offsetDateTime = OffsetDateTime.parse(cleanDate, formatter);
      return offsetDateTime.toLocalDateTime();
    } catch (DateTimeException | NullPointerException e) {
      logger.warn("Failed to parse V2 date: {}", date);
      return null;
    }
  }

  //2023-01-30 11:00:51
  public LocalDateTime parseDateV3(String date) {
    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
      return LocalDateTime.parse(date, formatter);
    } catch (DateTimeException e) {
      logger.warn("Failed to parse V3 date: {}", date);
      return null;
    }
  }

}
