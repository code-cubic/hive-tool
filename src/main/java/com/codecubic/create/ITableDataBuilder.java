package com.codecubic.create;

import com.codecubic.dao.JdbcTemplate;
import com.codecubic.model.TableMeta;

import java.sql.SQLException;
import java.util.Map;

public interface ITableDataBuilder extends Cloneable {
    void setJdbcTemplate(JdbcTemplate jdbcTemplate);

    void dataCreate(TableMeta tableMeta, Map<String,Object> partitionColValMap, int num, int batch) throws SQLException;
}
