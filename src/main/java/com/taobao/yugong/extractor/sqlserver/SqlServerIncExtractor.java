package com.taobao.yugong.extractor.sqlserver;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.model.ExtractStatus;
import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.IncrementOpType;
import com.taobao.yugong.common.model.record.IncrementRecord;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.exception.YuGongException;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.joda.time.DateTime;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.taobao.yugong.common.utils.YuGongToStringStyle.DEFAULT_DATE_TIME_PATTERN;

/**
 * Not thread safe
 *
 * @author JasonW
 */
public class SqlServerIncExtractor extends AbstractSqlServerExtractor {

    public static final int INC_MIN_DURATION = 10 * 60;
    public static final int WAITING_POLLING_SECONDS = 60;
    private final String schemaName;
    private final String tableName;
    private Table tableMeta;
    private List<ColumnMeta> primaryKeyMetas;
    private List<ColumnMeta> columnsMetas;
    private final YuGongContext context;
    private DateTime start;
    private final int noUpdateSleepTime;
    private final int stepTime;

    public SqlServerIncExtractor(YuGongContext context, DateTime start, int noUpdateSleepTime,
                                 int stepTime) {
        this.context = context;
        this.schemaName = context.getTableMeta().getSchema();
        this.tableName = context.getTableMeta().getName();
        this.start = start;
        this.noUpdateSleepTime = noUpdateSleepTime;
        this.stepTime = stepTime;
    }

    @Override
    public void start() {
        super.start();
        tableMeta = context.getTableMeta();
        primaryKeyMetas = tableMeta.getPrimaryKeys();
        columnsMetas = tableMeta.getColumns();
        tracer.update(context.getTableMeta().getFullName(), ProgressStatus.INCING);
    }

    @Override
    public List<Record> extract() {
        final DateTime now = DateTime.now();
        final DateTime end = start.plusSeconds(stepTime);
        if (end.isAfter(now.minusSeconds(WAITING_POLLING_SECONDS))) {
            setStatus(ExtractStatus.CATCH_UP);
            tracer.update(context.getTableMeta().getFullName(), ProgressStatus.SUCCESS);
            try {
                Thread.sleep(noUpdateSleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();// 传递下去
                return Lists.newArrayList();
            }
            return Lists.newArrayList();
        }

        logger.info("start {}, end {}", start, end);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getSourceDs());
        List<IncrementRecord> records = Lists.newArrayList();
        try {
            records = fetchSwdIncRecord(jdbcTemplate, primaryKeyMetas,
                    columnsMetas, start, end);
        } catch (BadSqlGrammarException e) {
            if (e.getCause().getMessage().equals("An insufficient number of arguments were supplied for the procedure or function cdc.fn_cdc_get_all_changes_ ... .")) {
                logger.info("An insufficient number counter, ignore");
            } else {
                logger.error("message is: {}", e.getMessage());
                throw new YuGongException(e);
            }
        }
        logger.info("processed ids: {}", Joiner.on(",").join(records.stream()
                .map(x -> Joiner.on("+").join(x.getPrimaryKeys().stream()
                        .map(x1 -> x1.getValue().toString()).collect(Collectors.toList()))).collect(Collectors.toList())));
        start = end;

        return (List<Record>) (List<? extends Record>) records;
    }

    @Override
    public Position ack(List<Record> records) {
        return null;
    }

    private List<IncrementRecord> fetchSwdIncRecord(JdbcTemplate jdbcTemplate,
                                                    final List<ColumnMeta> primaryKeysM, final List<ColumnMeta> columnsM,
                                                    DateTime start, DateTime end)
            throws BadSqlGrammarException, YuGongException {
        SimpleDateFormat format = new SimpleDateFormat(DEFAULT_DATE_TIME_PATTERN);

        String sql = String.format(
                "SELECT * from __history__ where ts > '%s' and ts < '%s' and table_name = '%s'",
                format.format(start.toDate()),
                format.format(end.toDate()),
                tableName
        );

        List<IncrementRecord> records = Lists.newArrayList();

        jdbcTemplate.execute(sql, (PreparedStatement ps) -> {
            ResultSet resultSet = ps.executeQuery();

            while (resultSet.next()) {

                List<ColumnValue> columnValues = Lists.newArrayList();
                List<ColumnValue> primaryKeys = Lists.newArrayList();

                int operationValue = (Integer) (YuGongUtils.getColumnValue(resultSet, null,
                        new ColumnMeta("state", Types.INTEGER)).getValue());
                Optional<IncrementOpType> operation = IncrementOpType.ofSqlServerInc(operationValue);
                if (!operation.isPresent()) {
                    continue;
                }

                String pkDataDest = resultSet.getString("pk_data_dest");

                try {
                    Document pkData = DocumentHelper.parseText(pkDataDest);
                    String pk = pkData.getRootElement().getQName().getName();
                    String pkValue = pkData.getStringValue();

                    // ### Begin Handle Delete operation
                    if (IncrementOpType.isSqlServerIncDelete(operationValue)) {

                        ColumnValue columnValue = new ColumnValue();
                        columnValue.setColumn(new ColumnMeta(pk, JDBCType.BIGINT.ordinal()));
                        columnValue.setValue(Integer.valueOf(pkValue));
                        primaryKeys.add(columnValue);
                        columnValues.add(columnValue);

                        IncrementRecord record = new IncrementRecord(
                                context.getTableMeta().getSchema(),
                                context.getTableMeta().getName(),
                                primaryKeys, columnValues, operation.get());
                        records.add(record);

                    } else { // ### End Handle Delete operation

                        // ### Begin Handle Insert or Update operation
                        String actSql = String.format("SELECT * from %s where %s = %s", tableName, pk, pkValue);

                        jdbcTemplate.execute(actSql, (PreparedStatement actPs) -> {

                            ResultSet actResultSet = actPs.executeQuery();

                            while (actResultSet.next()) {
                                for (ColumnMeta primaryKey : primaryKeysM) {
                                    ColumnValue columnValue = YuGongUtils.getColumnValue(actResultSet, null, primaryKey);
                                    primaryKeys.add(columnValue);
                                }

                                for (ColumnMeta column : columnsM) {
                                    ColumnValue columnValue = YuGongUtils.getColumnValue(actResultSet, null, column);
                                    columnValues.add(columnValue);
                                }

                                IncrementRecord record = new IncrementRecord(
                                        context.getTableMeta().getSchema(),
                                        context.getTableMeta().getName(),
                                        primaryKeys, columnValues, operation.get());
                                records.add(record);
                            }

                            return null;
                        });

                        // ### End Handle Insert or Update operation
                    }

                } catch (DocumentException e) {
                    throw new YuGongException(e);
                }
            }

            return null;
        });
        return records;
    }

    @Override
    public void stop() {
        super.stop();

        tracer.update(context.getTableMeta().getFullName(), ProgressStatus.SUCCESS);
    }

}
