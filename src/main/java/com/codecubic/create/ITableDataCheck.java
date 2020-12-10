package com.codecubic.create;

import com.codecubic.dao.JdbcTemplate;
import com.codecubic.model.TableMeta;

import java.util.Map;

public interface ITableDataCheck extends Cloneable {
    void setJdbcTemplate(JdbcTemplate jdbcTemplate);

    boolean dataCheck(TableMeta tableMeta, Map<String, Object> partitionColValMap,long count);
}
