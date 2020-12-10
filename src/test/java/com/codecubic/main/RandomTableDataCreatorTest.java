package com.codecubic.main;

import com.codecubic.model.AppConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

class RandomTableDataCreatorTest {

    @Test
    void randomDataCreate() {
        System.setProperty("HADOOP_USER_NAME", "hive");
        System.setProperty("HADOOP_HOME", "D:\\local\\hadoop-3.1.3");
        System.setProperty("hadoop.home.dir", "D:\\local\\hadoop-3.1.3");

        Yaml yaml = new Yaml();
        AppConf appConf = yaml.loadAs(AppConf.class.getClassLoader().getResourceAsStream("application.yaml"), AppConf.class);

        boolean suss = RandomTableDataCreator.randomDataCreate(appConf);

        Assertions.assertTrue(suss);

    }
}
