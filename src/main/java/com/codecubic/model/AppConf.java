package com.codecubic.model;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@ToString
@Data
public class AppConf {
    private String database;
    private String tableName;
    private int num;
    private int batch;
    private List<String> pkCols = new ArrayList<>();
    private Map<String, Object> partitionColVals = new HashMap<>();
    private JdbcConf jdbcConf;
}
