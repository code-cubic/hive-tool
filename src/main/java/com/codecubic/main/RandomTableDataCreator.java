package com.codecubic.main;

import com.codecubic.create.BaseDataCreator;
import com.codecubic.create.RandomTableDataBuilder;
import com.codecubic.create.SimpleTableDataCheck;
import com.codecubic.create.SimpleTableManager;
import com.codecubic.model.AppConf;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RandomTableDataCreator {
    public static boolean randomDataCreate(AppConf appConf) {
        log.info("appConf:{}", appConf);

        BaseDataCreator creator = BaseDataCreator.Builder.get()
                .setTableManager(new SimpleTableManager())
                .setTableDataBuilder(new RandomTableDataBuilder())
                .setTableDataCheck(new SimpleTableDataCheck())
                .setJdbcConf(appConf.getJdbcConf())
                .build();
        try {
            creator.createData(appConf);
            return true;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            creator.close();
        }
        return false;
    }
}
