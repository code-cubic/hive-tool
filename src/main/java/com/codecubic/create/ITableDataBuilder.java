package com.codecubic.create;

import com.codecubic.dao.JdbcTemplate;
import com.codecubic.model.TableMeta;

import java.util.Map;

public interface ITableDataBuilder extends Cloneable {
    void setJdbcTemplate(JdbcTemplate jdbcTemplate);

    boolean dataCreate(TableMeta tableMeta, Map<String,Object> partitionColValMap, int num, int batch);

    void close();
}
