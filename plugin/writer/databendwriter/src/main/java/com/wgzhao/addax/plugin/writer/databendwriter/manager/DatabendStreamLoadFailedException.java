package com.wgzhao.addax.plugin.writer.databendwriter.manager;

import java.io.IOException;
import java.util.Map;

public class DatabendStreamLoadFailedException
        extends IOException
{

    static final long serialVersionUID = 1L;

    private final Map<String, Object> response;
    private boolean reCreateLabel;

    public DatabendStreamLoadFailedException(String message, Map<String, Object> response)
    {
        super(message);
        this.response = response;
    }

    public DatabendStreamLoadFailedException(String message, Map<String, Object> response, boolean reCreateLabel)
    {
        super(message);
        this.response = response;
        this.reCreateLabel = reCreateLabel;
    }

    public Map<String, Object> getFailedResponse()
    {
        return response;
    }

    public boolean needReCreateLabel()
    {
        return reCreateLabel;
    }
}
