package com.codecubic.create;

import com.codecubic.dao.JdbcTemplate;
import com.codecubic.model.TableMeta;

import java.sql.SQLException;

public interface ITableManager {

    void setJdbcTemplate(JdbcTemplate jdbcTemplate);

    TableMeta getTable(String database, String tableName);

    boolean createTmpTable(String database, String tmpTableName);

    void dropTable(String database, String tmpTableName);

    void createTable(TableMeta tableMeta) throws SQLException;
}
