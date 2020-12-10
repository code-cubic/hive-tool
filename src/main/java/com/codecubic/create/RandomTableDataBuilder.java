package com.codecubic.create;

import com.codecubic.dao.JdbcTemplate;
import com.codecubic.model.ColumnMeta;
import com.codecubic.model.TableMeta;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.lang.String.join;

@Slf4j
public class RandomTableDataBuilder implements ITableDataBuilder {
    @Setter
    private JdbcTemplate jdbcTemplate;

    @Override
    public void dataCreate(TableMeta tableMeta, Map<String, Object> partitionColValMap, int num, int batch) throws SQLException {
        String database = tableMeta.getDatabase();
        String name = tableMeta.getName();
        String tmpTableName = name + "_tmp";
        List<ColumnMeta> colMetas = tableMeta.getColMetas();
        List<ColumnMeta> normalCols = colMetas.stream().filter(c -> !c.isPartitionCol()).collect(Collectors.toList());
        List<ColumnMeta> partitionCols = colMetas.stream().filter(e -> e.isPartitionCol()).collect(Collectors.toList());
        List<ColumnMeta> pkCols = colMetas.stream().filter(e -> e.isPkCol()).collect(Collectors.toList());
        List<String> pkNames = pkCols.stream().map(c -> c.getName()).collect(Collectors.toList());
        String pExprs = "";
        if (!partitionCols.isEmpty()) {
            List<String> pColVals = partitionCols.stream().map(e -> {
                Object colVal = partitionColValMap.get(e.getName());
                colVal = colVal == null ? UUID.randomUUID() : colVal;
                if (e.getType().equalsIgnoreCase("string")) {
                    colVal = "'" + colVal + "'";
                }
                return e.getName() + " = " + colVal.toString();
            }).collect(Collectors.toList());
            pExprs = "partition(" + join(",", pColVals) + ")";
        }

        List<String> randomColVals = new ArrayList<>(normalCols.size());
        List<String> selCols = new ArrayList<>(normalCols.size());

        for (ColumnMeta col : normalCols) {
            if (col.getType().equalsIgnoreCase("string")) {
                randomColVals.add(format("concat('" + col.getName() + "_',ceiling(rand()*%s)) as %s", pkNames.contains(col.getName()) ? Integer.MAX_VALUE : 100, col.getName()));
            } else {
                randomColVals.add(format("ceiling(rand()*%s) as %s", Integer.MAX_VALUE, col.getName()));
            }
            selCols.add("max(" + col.getName() + ")");
        }

        List<String> baseTabs = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            String subSql = "";
            if (i > 0) {
                subSql = " join ";
            }
            //增大数据量，以防重复数据导致最后的结果条数不对
            int size = (int) (batch * 1.5);
            subSql = subSql + format("(select '' from %s.%s limit %s) %s_%s", database, tmpTableName, size, tmpTableName, i);
            if (i > 0) {
                subSql += " on 1=1 ";
            }
            baseTabs.add(subSql);
        }

        String insertTemplat = "insert overwrite table %s.%s %s select %s from (select %s from %s) T group by %s limit %s";
        String overwriteSql = format(insertTemplat, database, name, pExprs,
                join(",", selCols),
                join(",", randomColVals),
                join("", baseTabs),
                join(",", pkNames),
                num * batch);

        log.info("overwriteSql:{}", overwriteSql);

        jdbcTemplate.execute(overwriteSql);
    }
}
