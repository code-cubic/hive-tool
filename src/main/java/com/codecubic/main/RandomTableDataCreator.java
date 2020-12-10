package com.codecubic.main;

import com.codecubic.create.BaseDataCreator;
import com.codecubic.create.RandomTableDataBuilder;
import com.codecubic.create.SimpleTableDataCheck;
import com.codecubic.create.SimpleTableManager;
import com.codecubic.exception.TableNotFound;
import com.codecubic.model.JdbcConfig;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

@Slf4j
public class RandomTableDataCreator {
    public static void main(String[] args) throws TableNotFound, SQLException {
        JdbcConfig jdbcConfig = new JdbcConfig();
        jdbcConfig.setName("hive");
        jdbcConfig.setUser("cpp");
        jdbcConfig.setPasswd("cpp");
        jdbcConfig.setDriver("org.apache.hive.jdbc.HiveDriver");
        jdbcConfig.setUrl("jdbc:hive2://bxzj-test-swift0.bxzj.baixinlocal.com:2181,bxzj-test-swift1.bxzj.baixinlocal.com:2181,bxzj-test-swift2.bxzj.baixinlocal.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2");
        BaseDataCreator creator = BaseDataCreator.Builder.get()
                .setTableManager(new SimpleTableManager())
                .setTableDataBuilder(new RandomTableDataBuilder())
                .setTableDataCheck(new SimpleTableDataCheck())
                .setJdbcConfig(jdbcConfig)
                .build();
        creator.createData("cpp_c", "wh_20201201");
        creator.close();
    }
}
