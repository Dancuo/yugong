package com.taobao.yugong.common.stats;

import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.RunMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 统计下当前各表迁移的状态
 *
 * @author agapple 2014-4-24 下午2:12:13
 * @since 3.0.4
 */
public class ProgressTracer {

  private static final Logger logger = LoggerFactory.getLogger(ProgressTracer.class);
  private static final String FULL_FORMAT = "{Waiting:%s, On Full:%s, Finished:%s, Exception:%s}";
  private static final String INC_FORMAT = "{Waiting:%s, On Inc:%s, Caught:%s, Exception:%s}";
  private static final String CHECK_FORMAT = "{Waiting:%s, On Compare:%s, Finished:%s, Exception:%s}";
  private static final String ALL_FORMAT = "{Waiting:%s,  On Full:%s, On Inc:%s, Caught:%s, Exception:%s}";

  private int total;
  private RunMode mode;
  private Map<String, ProgressStatus> status = new ConcurrentHashMap<>();

  public ProgressTracer(RunMode mode, int total) {
    this.mode = mode;
    this.total = total;
  }

  public void update(String tableName, ProgressStatus progress) {
    ProgressStatus st = status.get(tableName);
    if (st != ProgressStatus.FAILED) {
      status.put(tableName, progress);
    }
  }

  public void printSummry() {
    print(false);
  }

  public void print(boolean detail) {
    int fulling = 0;
    int incing = 0;
    int failed = 0;
    int success = 0;
    List<String> fullingTables = new ArrayList<>();
    List<String> incingTables = new ArrayList<>();
    List<String> failedTables = new ArrayList<>();
    List<String> successTables = new ArrayList<>();

    for (Map.Entry<String, ProgressStatus> entry : status.entrySet()) {
      ProgressStatus progress = entry.getValue();
      if (progress == ProgressStatus.FULLING) {
        fulling++;
        fullingTables.add(entry.getKey());
      } else if (progress == ProgressStatus.INCING) {
        incing++;
        incingTables.add(entry.getKey());
      } else if (progress == ProgressStatus.FAILED) {
        failed++;
        failedTables.add(entry.getKey());
      } else if (progress == ProgressStatus.SUCCESS) {
        success++;
        successTables.add(entry.getKey());
      }
    }

    int unknow = this.total - fulling - incing - failed - success;
    String msg = null;
    if (mode == RunMode.ALL) {
      msg = String.format(ALL_FORMAT, unknow, fulling, incing, success, failed);
    } else if (mode == RunMode.FULL) {
      msg = String.format(FULL_FORMAT, unknow, fulling, success, failed);
    } else if (mode == RunMode.INC) {
      msg = String.format(INC_FORMAT, unknow, incing, success, failed);
    } else if (mode == RunMode.CHECK) {
      msg = String.format(CHECK_FORMAT, unknow, fulling, success, failed);
    }

    logger.info("{}", msg);
    if (detail) {
      if (fulling > 0) {
        if (mode == RunMode.CHECK) {
          logger.info("On Compare:" + fullingTables);
        } else {
          logger.info("On Full:" + fullingTables);
        }
      }
      if (incing > 0) {
        logger.info("On Inc:" + incingTables);
      }
      if (failed > 0) {
        logger.info("Exception:" + failedTables);
      }
      logger.info("Finished:" + successTables);
    }
  }
}
