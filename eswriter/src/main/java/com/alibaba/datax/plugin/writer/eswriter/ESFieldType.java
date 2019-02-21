package com.alibaba.datax.plugin.writer.eswriter;

/**
 * Created by xiongfeng.bxf on 17/3/1.
 */
public enum ESFieldType {
    ID,
    INDEX,
    ROUTING,
    QUERY,
    NEXT_COLUMN_NAME,

    STRING,
    TEXT,
    KEYWORD,
    LONG,
    INTEGER,
    SHORT,
    BYTE,
    DOUBLE,
    FLOAT,
    DATE,
    BOOLEAN,
    BINARY,
    GEO_POINT,
    GEO_SHAPE,
    IP,

    ARRAY,
    OBJECT,
    NESTED;

    public static ESFieldType getESFieldType(String type) {
        if (type == null) {
            return null;
        }
        for (ESFieldType f : ESFieldType.values()) {
            if (f.name().compareTo(type.toUpperCase()) == 0) {
                return f;
            }
        }
        return null;
    }
}
