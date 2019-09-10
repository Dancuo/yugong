package com.taobao.yugong.common.utils;

import org.apache.commons.lang.builder.ToStringStyle;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 统一的ToStringStyle
 *
 * @author agapple 2010-6-18 上午11:35:27
 */
public class YuGongToStringStyle extends ToStringStyle {

  private static final long serialVersionUID = -6568177374288222145L;

  public static final String DEFAULT_DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
  public static final ToStringStyle DEFAULT_STYLE = new DateStyle(DEFAULT_DATE_TIME_PATTERN);
  public static final DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = DateTimeFormat.forPattern(DEFAULT_DATE_TIME_PATTERN);

  private static class DateStyle extends ToStringStyle {

    private static final long serialVersionUID = 5208917932254652886L;

    private String datePattern;

    public DateStyle(String datePattern) {
      super();
      this.setUseIdentityHashCode(false);
      this.setUseShortClassName(true);
      this.datePattern = datePattern;
    }

    @Override
    protected void appendDetail(StringBuffer buffer, String fieldName, Object value) {
      if (value instanceof Date) {
        value = new SimpleDateFormat(datePattern).format(value);
      } else {
        buffer.append(value);
      }
    }
  }
}
