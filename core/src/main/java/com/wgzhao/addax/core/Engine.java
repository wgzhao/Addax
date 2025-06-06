/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.core;

import com.wgzhao.addax.core.element.ColumnCast;
import com.wgzhao.addax.core.statistics.VMInfo;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.job.JobContainer;
import com.wgzhao.addax.core.util.ConfigParser;
import com.wgzhao.addax.core.util.ConfigurationValidate;
import com.wgzhao.addax.core.util.container.LoadUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
import java.util.Set;

/**
 * Engine is the entry class of Addax. This class is responsible for initializing the running container of Job or Task,
 * and running the Job or Task logic of the plugin.
 */
public class Engine
{
    private static final Logger LOG = LoggerFactory.getLogger(Engine.class);

    public void start(Configuration allConf)
    {

        // bind and casting column type
        ColumnCast.bind(allConf);

        // initialize the PluginLoader
        LoadUtil.bind(allConf);

        AbstractContainer container;
        container = new JobContainer(allConf);

        container.start();
    }


    public static String filterJobConfiguration(final Configuration configuration)
    {
        Configuration jobConfWithSetting = configuration.getConfiguration("job").clone();

        Configuration jobContent = jobConfWithSetting.getConfiguration("content");

        filterSensitiveConfiguration(jobContent);

        jobConfWithSetting.set("content", jobContent);

        return jobConfWithSetting.beautify();
    }

    public static void filterSensitiveConfiguration(Configuration configuration)
    {
        Set<String> keys = configuration.getKeys();
        for (String key : keys) {
            boolean isSensitive = StringUtils.endsWithIgnoreCase(key, "password")
                    || StringUtils.endsWithIgnoreCase(key, "accessKey")
                    || StringUtils.endsWithIgnoreCase(key, "token");
            if (isSensitive && configuration.get(key) instanceof String) {
                configuration.set(key, "*****");
            }
        }
    }

    public static void entry(String[] args)
            throws Throwable
    {
        Options options = new Options();
        options.addOption("job", true, "Job config.");

        DefaultParser parser = new DefaultParser();
        CommandLine cl = parser.parse(options, args);

        String jobPath = cl.getOptionValue("job");
        Configuration configuration = ConfigParser.parse(jobPath);

        //打印vmInfo
        VMInfo vmInfo = VMInfo.getVmInfo();
        if (vmInfo != null) {
            LOG.debug(vmInfo.toString());
        }

        LOG.info("\n{}\n", Engine.filterJobConfiguration(configuration));

        LOG.debug(configuration.toJSON());

        ConfigurationValidate.doValidate(configuration);
        Engine engine = new Engine();
        engine.start(configuration);
    }

    public static String getVersion()
    {
        try {
            final Properties properties = new Properties();
            properties.load(Engine.class.getClassLoader().getResourceAsStream("project.properties"));
            return properties.getProperty("version");
        }
        catch (IOException e) {
            return null;
        }
    }

    public static void main(String[] args)
    {
        LOG.info("\n  ___      _     _            \n" +
                " / _ \\    | |   | |           \n" +
                "/ /_\\ \\ __| | __| | __ ___  __\n" +
                "|  _  |/ _` |/ _` |/ _` \\ \\/ /\n" +
                "| | | | (_| | (_| | (_| |>  < \n" +
                "\\_| |_/\\__,_|\\__,_|\\__,_/_/\\_\\\n" +
                ":: Addax version ::    (v{})", Engine.getVersion());
        if (args.length < 2) {
            LOG.error("need a job file");
            System.exit(1);
        }
        try {
            Engine.entry(args);
        }
        catch (Throwable e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            LOG.error(sw.toString());
            System.exit(2);
        }
        System.exit(0);
    }
}
