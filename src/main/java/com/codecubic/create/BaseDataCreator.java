package com.codecubic.create;

import com.alibaba.fastjson.JSONObject;
import com.codecubic.dao.JdbcTemplate;
import com.codecubic.exception.TableCreateException;
import com.codecubic.exception.TableDataBuildException;
import com.codecubic.exception.TableDataCheckException;
import com.codecubic.exception.TableNotFound;
import com.codecubic.model.AppConf;
import com.codecubic.model.JdbcConf;
import com.codecubic.model.TableMeta;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Data
@Slf4j
public class BaseDataCreator implements Cloneable {
    private ITableManager tableManager;
    private ITableDataBuilder tableDataBuilder;
    private ITableDataCheck tableDataCheck;
    private String database;
    private String tableName;
    private String tmpTableName;


    @Data
    @Slf4j
    public static class Builder {

        private ITableManager tableManager;
        private ITableDataBuilder tableDataBuilder;
        private ITableDataCheck tableDataCheck;
        private JdbcConf jdbcConf;

        public Builder setTableManager(ITableManager tableManager) {
            this.tableManager = tableManager;
            return this;
        }

        public Builder setTableDataBuilder(ITableDataBuilder tableDataBuilder) {
            this.tableDataBuilder = tableDataBuilder;
            return this;
        }

        public Builder setTableDataCheck(ITableDataCheck tableDataCheck) {
            this.tableDataCheck = tableDataCheck;
            return this;
        }

        public Builder setJdbcConf(JdbcConf jdbcConf) {
            this.jdbcConf = jdbcConf;
            return this;
        }

        private Builder() {
        }

        public static Builder get() {
            return new Builder();
        }

        public BaseDataCreator build() {

            JdbcTemplate jdbcTemplate = new JdbcTemplate(jdbcConf);
            this.tableManager.setJdbcTemplate(jdbcTemplate);
            this.tableDataBuilder.setJdbcTemplate(jdbcTemplate);
            this.tableDataCheck.setJdbcTemplate(jdbcTemplate);
            BaseDataCreator baseDataCreator = new BaseDataCreator();
            baseDataCreator.setTableManager(this.tableManager);
            baseDataCreator.setTableDataBuilder(this.tableDataBuilder);
            baseDataCreator.setTableDataCheck(this.tableDataCheck);

            return baseDataCreator;
        }
    }


    public void createData(AppConf appConf) throws TableNotFound, TableCreateException, TableDataCheckException, TableDataBuildException {
        this.database = appConf.getDatabase();
        this.tableName = appConf.getTableName();
        this.tmpTableName = tableName + "_tmp";

        List<String> pkCols = appConf.getPkCols();
        List<Map<String, Object>> partitionColVals = appConf.getPartitionColVals();
        int batch = appConf.getBatch();
        int num = appConf.getNum();
        long total = num * batch;

        TableMeta table = tableManager.getTable(database, tableName);
        if (pkCols != null) {
            table.getColMetas().forEach(col -> {
                if (pkCols.contains(col.getName())) {
                    col.setPkCol(true);
                }
            });
        }

        if (table.getName() == null) {
            throw new TableNotFound(database + "." + tableName);
        }
        TableMeta tmpTable = tableManager.getTable(database, tmpTableName);
        if (tmpTable.getName() == null) {
            boolean suss = tableManager.createTmpTable(database, tmpTableName);
            if (!suss) {
                throw new TableCreateException();
            }
        }

        for (Map<String, Object> map : partitionColVals) {
            boolean builderSuss = this.tableDataBuilder.dataCreate(table, map, num, batch);
            if (builderSuss) {
                boolean checkSuss = this.tableDataCheck.dataCheck(table, map, total);
                if (!checkSuss) {
                    throw new TableDataCheckException(JSONObject.toJSONString(map));
                }
            } else {
                throw new TableDataBuildException(JSONObject.toJSONString(map));
            }
        }
    }

    public void close() {
        tableManager.dropTable(database, this.tmpTableName);
        tableManager.close();
    }
}
