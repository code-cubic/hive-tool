package com.codecubic.create;

import com.codecubic.dao.JdbcTemplate;
import com.codecubic.model.ColumnMeta;
import com.codecubic.model.TableMeta;
import com.codecubic.util.TimeUtil;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;

import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.lang.String.join;

@Slf4j
public class RandomTableDataBuilder implements ITableDataBuilder {
    @Setter
    private JdbcTemplate jdbcTemplate;

    @Override
    public boolean dataCreate(TableMeta tableMeta, Map<String, Object> partitionColValMap, int num, int batch) {
        try {
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
                boolean isPk = pkNames.contains(col.getName());
                long randomSalt = isPk ? Integer.MAX_VALUE : RandomUtils.nextInt(1, 300);
                String cType = col.getType().toLowerCase();
                cType = cType.startsWith("decimal(") ? "double" : cType;
                switch (cType) {
                    case "string":
                        //非pk col 不加前缀，主要是方便列式分区表的字段值进行类型转换，防止该格式下字符串不能转换成其他格式
                        randomColVals.add(format("concat('%s',ceiling(rand()*%s)) as %s", isPk ? col.getName() : "", randomSalt, col.getName()));
                        break;
                    case "double":
                        randomColVals.add(format("round(rand()*%s,4) as %s", randomSalt, col.getName()));
                        break;
                    case "date":
                        randomColVals.add(format("date_add('%s',cast(rand()*%s as int)) as %s",
                                TimeUtil.date2Str(new Date(), TimeUtil.YYYY) + "-01-01",
                                randomSalt, col.getName()));
                        break;
                    case "timestamp":
                        randomColVals.add(format("date_add('%s',cast(rand()*%s as int)) as %s",
                                TimeUtil.date2Str(new Date(), TimeUtil.YYYY) + "-01-01",
                                randomSalt, col.getName()));
                        break;
                    default:
                        randomColVals.add(format("ceiling(rand()*%s) as %s", randomSalt, col.getName()));

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

        } catch (Exception e) {
            log.error("", e);
            return false;
        }
        return true;
    }

    @Override
    public void close() {

    }
}
