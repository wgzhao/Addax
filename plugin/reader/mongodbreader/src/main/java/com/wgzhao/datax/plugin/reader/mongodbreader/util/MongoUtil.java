package com.wgzhao.datax.plugin.reader.mongodbreader.util;

import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.plugin.reader.mongodbreader.KeyConstant;
import com.wgzhao.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/17 0017.
 * Modified by mingyan.zc on 2016/6/13.
 */
public class MongoUtil
{

    private MongoUtil() {}

    public static MongoClient initMongoClient(Configuration conf)
    {

        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
        if (addressList == null || addressList.isEmpty()) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
        }
        try {
            return new MongoClient(parseServerAddress(addressList));
        }
        catch (UnknownHostException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_ADDRESS, "不合法的地址");
        }
        catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
        }
        catch (Exception e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.UNEXCEPT_EXCEPTION, "未知异常");
        }
    }

    public static MongoClient initCredentialMongoClient(Configuration conf, String userName, String password, String database)
    {

        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
        if (!isHostPortPattern(addressList)) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
        }
        try {
            MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
            return new MongoClient(parseServerAddress(addressList), Collections.singletonList(credential));
        }
        catch (UnknownHostException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_ADDRESS, "不合法的地址");
        }
        catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, "不合法参数");
        }
        catch (Exception e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.UNEXCEPT_EXCEPTION, "未知异常");
        }
    }

    /**
     * 判断地址类型是否符合要求
     *
     * @param addressList host list
     * @return boolean
     */
    private static boolean isHostPortPattern(List<Object> addressList)
    {
        for (Object address : addressList) {
            String regex = "(\\S+):([0-9]+)";
            if (!((String) address).matches(regex)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 转换为mongo地址协议
     *
     * @param rawAddressList raw address list
     * @return List of ServerAddress
     * @throws UnknownHostException can not find host or ip address
     */
    private static List<ServerAddress> parseServerAddress(List<Object> rawAddressList)
            throws UnknownHostException
    {
        List<ServerAddress> addressList = new ArrayList<>();
        for (Object address : rawAddressList) {
            String[] tempAddress = ((String) address).split(":");
            try {
                ServerAddress sa = new ServerAddress(tempAddress[0], Integer.parseInt(tempAddress[1]));
                addressList.add(sa);
            }
            catch (Exception e) {
                throw new UnknownHostException();
            }
        }
        return addressList;
    }
}
