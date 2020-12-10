package com.codecubic.create;

import com.codecubic.dao.JdbcTemplate;
import com.codecubic.model.TableMeta;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

@Slf4j
public class SimpleTableDataCheck implements ITableDataCheck {

    @Setter
    private JdbcTemplate jdbcTemplate;

    @Override
    public boolean dataCheck(TableMeta tableMeta, Map<String, Object> partitionColValMap, long count) {

        String condition = "";
        if (partitionColValMap != null) {
            List<String> exprs = new ArrayList<>(partitionColValMap.size());
            partitionColValMap.forEach((k, v) -> {
                if (v instanceof String) {
                    exprs.add(format(" %s = '%s'", k, v));
                } else {
                    exprs.add(format(" %s = %s", k, v));
                }
            });
            condition = " where " + String.join(" and ", exprs);
        }
        String countSql = format("select count(1) from %s.%s %s", tableMeta.getDatabase(),
                tableMeta.getName(), condition);
        log.info("countSql:{}", countSql);
        return count == jdbcTemplate.count(countSql);
    }
}
