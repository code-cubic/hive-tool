package com.codecubic.main;

import com.codecubic.model.AppConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RandomTableDataCreatorTest {

    /**
     * 测试单分区普通表
     */
    @Test
    void randomDataCreate01() {
        System.setProperty("HADOOP_USER_NAME", "hive");
        System.setProperty("HADOOP_HOME", "D:\\local\\hadoop-3.1.3");
        System.setProperty("hadoop.home.dir", "D:\\local\\hadoop-3.1.3");

        Yaml yaml = new Yaml();
        AppConf appConf = yaml.loadAs(AppConf.class.getClassLoader().getResourceAsStream("application.yaml"), AppConf.class);

        boolean suss = RandomTableDataCreator.randomDataCreate(appConf);

        Assertions.assertTrue(suss);

    }

    /**
     * 测试多分区普通表
     */
    @Test
    void randomDataCreate02() {
        System.setProperty("HADOOP_USER_NAME", "hive");
        System.setProperty("HADOOP_HOME", "D:\\local\\hadoop-3.1.3");
        System.setProperty("hadoop.home.dir", "D:\\local\\hadoop-3.1.3");

        Yaml yaml = new Yaml();
        AppConf appConf = yaml.loadAs(AppConf.class.getClassLoader().getResourceAsStream("application.yaml"), AppConf.class);

        appConf.setTableName("bdp_cid_user_label_snap");
        List<Map<String, Object>> partitionColVals = appConf.getPartitionColVals();
        partitionColVals.clear();
        Map<String, Object> cond1 = new HashMap<>();
        cond1.putIfAbsent("etl_dt", "20200923");
        cond1.putIfAbsent("field", "bal_deposit");
        partitionColVals.add(cond1);
        Map<String, Object> cond2 = new HashMap<>();
        cond2.putIfAbsent("etl_dt", "20200923");
        cond2.putIfAbsent("field", "lstentr_time_app");
        partitionColVals.add(cond2);

        boolean suss = RandomTableDataCreator.randomDataCreate(appConf);

        Assertions.assertTrue(suss);

    }
}
