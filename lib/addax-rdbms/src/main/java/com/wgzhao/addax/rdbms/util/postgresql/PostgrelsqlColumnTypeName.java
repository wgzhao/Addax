package com.wgzhao.addax.rdbms.util.postgresql;

import java.util.Objects;
import java.util.Optional;

public class PostgrelsqlColumnTypeName
{

    public static final String GEOMETRY = "geometry";

    public static boolean isGeometry(String columnTypeName)
    {
        return columnTypeName.equals(GEOMETRY);
    }

    public static boolean isArray(String columnTypeName)
    {
        if (Objects.isNull(columnTypeName)) {
            return false;
        }
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
