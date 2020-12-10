package com.codecubic.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@ToString
public class TableMeta {
    /**
     * 归属数据库
     */
    private String database;
    /**
     * 表名
     */
    private String name;
    /**
     *
     */
    private String type;
    /**
     * 表所属用户名
     */
    private String userName;
    /**
     * 表备注
     */
    private String remark;

    private List<ColumnMeta> colMetas = new ArrayList<>();

    public void addColMeta(ColumnMeta cm) {
        this.colMetas.add(cm);
    }

}
