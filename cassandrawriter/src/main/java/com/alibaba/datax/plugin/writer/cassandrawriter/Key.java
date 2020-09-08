package com.alibaba.datax.plugin.writer.cassandrawriter;

/**
 * Created by mazhenlin on 2019/8/19.
 */
public class Key {
  public final static String USERNAME = "username";
  public final static String PASSWORD = "password";

  public final static String HOST = "host";
  public final static String PORT = "port";
  public final static String USESSL = "useSSL";

  public final static String KEYSPACE = "keyspace";
  public final static String TABLE = "table";
  public final static String COLUMN = "column";
  public final static String WRITE_TIME = "writetime()";
  public final static String ASYNC_WRITE = "asyncWrite";
  public final static String CONSITANCY_LEVEL = "consistancyLevel";
  public final static String CONNECTIONS_PER_HOST = "connectionsPerHost";
  public final static String MAX_PENDING_CONNECTION = "maxPendingPerConnection";
  /**
   * 异步写入的批次大小，默认1（不异步写入）
   */
  public final static String BATCH_SIZE = "batchSize";

}
