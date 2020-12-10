package com.codecubic.create;

import com.codecubic.dao.JdbcTemplate;
import com.codecubic.model.ColumnMeta;
import com.codecubic.model.TableMeta;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.lang.String.join;

@Slf4j
public class SimpleTableManager implements ITableManager {
    @Setter
    private JdbcTemplate jdbcTemplate;

    @Override
    public TableMeta getTable(String database, String tableName) {
        try {
            return this.jdbcTemplate.getTabMeta(database, tableName);
        } catch (SQLException e) {
            log.error("", e);
        }
        return new TableMeta();
    }

    @Override
    public boolean createTmpTable(String database, String tmpTableName) {
        log.info("start createTmpTable:{}.{}", database, tmpTableName);
        //加载基表
        List<String> baseTabDatas = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            baseTabDatas.add(format(" select 1 as col_1 "));
        }
        try {
            this.jdbcTemplate.execute(format("drop table %s.%s", database, tmpTableName));
            String tmpTablSql = format("create table %s.%s as select col_1 from (%s) temp", database, tmpTableName, String.join("union all", baseTabDatas));
            log.info("tmpTablSql:{}", tmpTablSql);
            this.jdbcTemplate.execute(tmpTablSql);
            return true;
        } catch (Exception e) {
            log.error("", e);
            return false;
        }
    }

    @Override
    public void dropTable(String database, String tableName) {
        try {
            this.jdbcTemplate.execute(format("drop table %s.%s", database, tableName));
        } catch (SQLException e) {
            log.error("", e);
        }
    }

    @Override
    public void createTable(TableMeta tableMeta) throws SQLException {
        String database = tableMeta.getDatabase();
        List<ColumnMeta> colMetas = tableMeta.getColMetas();
        List<String> normalCols = colMetas.stream().filter(c -> !c.isPartitionCol()).map(e -> format("%s %s comment '%s'", e.getName(), e.getType(), e.getRemark())).collect(Collectors.toList());
        List<ColumnMeta> partitionCols = colMetas.stream().filter(e -> e.isPartitionCol()).collect(Collectors.toList());

        String pExprs = "";
        if (!partitionCols.isEmpty()) {
            List<String> pColVals = partitionCols.stream().map(e -> format("%s %s comment '%s'", e.getName(), e.getType(), e.getRemark())).collect(Collectors.toList());
            pExprs = "partitioned by (" + join(",", pColVals) + ")";
        }

        String createSql = format("create table %s.%s(%s) %s stored as parquet tblproperties ('parquet.compression'='snappy') ",
                database, tableMeta.getName(),
                String.join(",", normalCols),
                pExprs
        );
        log.info("createSql={}", createSql);
        jdbcTemplate.execute(createSql);
    }

    @Override
    public void close() {
        this.jdbcTemplate.close();
    }
}
