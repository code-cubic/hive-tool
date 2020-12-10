package com.codecubic.exception;

public class TableNotFound extends Exception {
    public TableNotFound(String s) {
        super(String.format("%s not exist!", s));
    }
}
