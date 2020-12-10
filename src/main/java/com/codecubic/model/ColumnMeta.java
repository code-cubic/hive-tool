package com.codecubic.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@Getter
@Setter
public class ColumnMeta {
    private String name;
    private String type;
    private String remark;
    private int size;
    private int digits;
    private boolean nullAble;
    private boolean partitionCol;
    private boolean pkCol;
}
