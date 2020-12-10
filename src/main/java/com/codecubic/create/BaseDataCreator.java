package com.codecubic.create;

import com.codecubic.dao.JdbcTemplate;
import com.codecubic.exception.TableNotFound;
import com.codecubic.model.JdbcConfig;
import com.codecubic.model.TableMeta;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.HashMap;
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
        private JdbcConfig jdbcConfig;

        private Builder() {
        }

        public static Builder get() {
            return new Builder();
        }

        public BaseDataCreator build() {

            JdbcTemplate jdbcTemplate = new JdbcTemplate(jdbcConfig);
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


    public boolean create(String database, String tableName) throws TableNotFound, SQLException {
        this.database = database;
        this.tableName = tableName;
        this.tmpTableName = tableName + "_tmp";

        TableMeta table = tableManager.getTable(database, tableName);
        if (table.getName() == null) {
            throw new TableNotFound(database + "." + tableName);
        }
        TableMeta tmpTable = tableManager.getTable(database, tmpTableName);
        if (tmpTable.getName() == null) {
            tableManager.createTmpTable(database, tmpTableName);
        }

        Map<String, Object> partitionColValMap = new HashMap<>();
        partitionColValMap.putIfAbsent("etl_dt", "20201201");

        this.tableDataBuilder.dataCreate(table, partitionColValMap, 2, 10);
        this.tableDataCheck.dataCheck(table, partitionColValMap, 2 * 10);

        return false;
    }

    public void close() {
        tableManager.dropTable(database, this.tmpTableName);
    }
}
