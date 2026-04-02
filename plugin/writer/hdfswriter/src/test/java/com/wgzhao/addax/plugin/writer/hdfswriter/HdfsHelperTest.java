package com.wgzhao.addax.plugin.writer.hdfswriter;

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HdfsHelperTest
{
    private final HdfsHelper helper = new HdfsHelper();

    @Test
    void shouldResolveBloomFilterConfiguration()
    {
        Configuration writerConfig = Configuration.from("""
                {
                  "bloom.filter.columns": ["user_id", "order_id"],
                  "bloom.filter.fpp": 0.01
                }
                """);
        List<Configuration> columns = List.of(
                Configuration.from("{\"name\":\"user_id\",\"type\":\"string\"}"),
                Configuration.from("{\"name\":\"order_id\",\"type\":\"bigint\"}")
        );

        HdfsHelper.BloomFilterConfig bloomFilterConfig =
                helper.resolveBloomFilterConfiguration(writerConfig, columns);

        assertNotNull(bloomFilterConfig);
        assertEquals("user_id,order_id", bloomFilterConfig.columns());
        assertEquals(0.01d, bloomFilterConfig.fpp(), 1.0e-12);
    }

    @Test
    void shouldUseDefaultFppWhenNotConfigured()
    {
        Configuration writerConfig = Configuration.from("""
                {
                  "bloom.filter.columns": ["user_id"]
                }
                """);
        List<Configuration> columns = List.of(
                Configuration.from("{\"name\":\"user_id\",\"type\":\"string\"}")
        );

        HdfsHelper.BloomFilterConfig bloomFilterConfig =
                helper.resolveBloomFilterConfiguration(writerConfig, columns);

        assertNotNull(bloomFilterConfig);
        assertEquals("user_id", bloomFilterConfig.columns());
        assertEquals(0.05d, bloomFilterConfig.fpp(), 1.0e-12);
    }

    @Test
    void shouldReturnNullWhenBloomFilterIsNotConfigured()
    {
        Configuration writerConfig = Configuration.from("{}");
        List<Configuration> columns = List.of(
                Configuration.from("{\"name\":\"user_id\",\"type\":\"string\"}")
        );

        assertNull(helper.resolveBloomFilterConfiguration(writerConfig, columns));
    }

    @Test
    void shouldRejectUnknownBloomColumn()
    {
        Configuration writerConfig = Configuration.from("""
                {
                  "bloom.filter.columns": ["missing_col"]
                }
                """);
        List<Configuration> columns = List.of(
                Configuration.from("{\"name\":\"user_id\",\"type\":\"string\"}")
        );

        assertThrows(AddaxException.class,
                () -> helper.resolveBloomFilterConfiguration(writerConfig, columns));
    }

    @Test
    void shouldRejectInvalidBloomFpp()
    {
        Configuration writerConfig = Configuration.from("""
                {
                  "bloom.filter.columns": ["user_id"],
                  "bloom.filter.fpp": 1.0
                }
                """);
        List<Configuration> columns = List.of(
                Configuration.from("{\"name\":\"user_id\",\"type\":\"string\"}")
        );

        assertThrows(AddaxException.class,
                () -> helper.resolveBloomFilterConfiguration(writerConfig, columns));
    }
}
