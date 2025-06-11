package com.wgzhao.addax.rdbms.util.postgresql;

import java.util.Optional;

public class PostgrelsqlColumnTypeName
{

    public static final String GEOMETRY = "geometry";

    public static final String TSRANGE = "tsrange";

    public static final String NUMRANGE = "numrange";

    public static final String INT4RANGE = "int4range";

    public static final String INT8RANGE = "int8range";

    public static final String TSTZRANGE = "tstzrange";

    public static final String DATERANGE = "daterange";

    public static final String JSONB = "jsonb";

    public static final String INET = "inet";

    public static final String TSVECTOR = "tsvector";

    public static boolean isPGObject(String columnTypeName)
    {
        return columnTypeName.equals(TSRANGE) ||
                columnTypeName.equals(TSTZRANGE) ||
                columnTypeName.equals(NUMRANGE) ||
                columnTypeName.equals(INT4RANGE) ||
                columnTypeName.equals(INT8RANGE) ||
                columnTypeName.equals(DATERANGE) ||
                columnTypeName.equals(JSONB) ||
                columnTypeName.equals(INET) ||
                columnTypeName.equals(TSVECTOR);
    }

    public static boolean isGeometry(String columnTypeName)
    {
        return columnTypeName.equals(GEOMETRY);
    }

    public static boolean isArray(String columnTypeName)
    {
        return columnTypeName.startsWith("_");
    }

    public static Optional<String> extractArrayType(String columnTypeName)
    {
        if (isArray(columnTypeName)) {
            return Optional.of(columnTypeName.substring(1));
        }
        return Optional.empty();
    }

}
