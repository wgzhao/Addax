package com.wgzhao.addax.rdbms.util;

import java.io.Serializable;

public class DataWrapper implements Serializable {

    private static final long serialVersionUID = 1L;

    private String columnTypeName;

    private Object rawData;

    public String getColumnTypeName() {
        return columnTypeName;
    }

    public void setColumnTypeName(String columnTypeName) {
        this.columnTypeName = columnTypeName;
    }

    public Object getRawData() {
        return rawData;
    }

    public void setRawData(Object rawData) {
        this.rawData = rawData;
    }
}
