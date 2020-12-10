package com.codecubic.create;

import com.codecubic.dao.JdbcTemplate;
import com.codecubic.model.TableMeta;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
        log.info("start createTmpTable:%s.%s", database, tmpTableName);
        //加载基表
        List<String> baseTabDatas = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            baseTabDatas.add(String.format(" select 1 as col_1 "));
        }
        try {
            this.jdbcTemplate.execute(String.format("drop table %s.%s", database, tmpTableName));
            String baseTabCreatSql = String.format("create table %s.%s as select col_1 from (%s) temp", database, tmpTableName, String.join("union all", baseTabDatas));
            this.jdbcTemplate.execute(baseTabCreatSql);
            return true;
        } catch (Exception e) {
            log.error("", e);
            return false;
        }
    }

    @Override
    public void dropTable(String database, String tableName) {
        try {
            this.jdbcTemplate.execute(String.format("drop table %s.%s", database, tableName));
        } catch (SQLException e) {
            log.error("", e);
        }
    }
}
