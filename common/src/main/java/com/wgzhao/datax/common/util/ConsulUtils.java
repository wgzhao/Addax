package com.wgzhao.datax.common.util;


import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.ConsulRawClient;
import com.ecwid.consul.v1.health.model.HealthService;

import java.util.List;


public class ConsulUtils {
    
    public static String getServiceLists(String host, int port, String serviceName) {
        ConsulRawClient client = new ConsulRawClient(host, port);
        ConsulClient consul = new ConsulClient(client);
        //获取所有服务
        List<HealthService> list = consul.getHealthServices(serviceName, false, null).getValue();

        String serviceIpAndPorts = "";

        for (HealthService s: list) {
            serviceIpAndPorts += s.getService().getAddress() +":"+ s.getService().getPort() + ",";
        }
        return serviceIpAndPorts.substring(0,serviceIpAndPorts.length() - 1);
    }

    // 获取单个服务的 IpAndPort,以字符串的形式返回
    public static String getSingleService(String host, int port, String serviceName) {
        return getServiceLists(host, port, serviceName).split(",")[0];
    }

    public static void main(String[] args) {
//        System.out.println(getServiceLists("10.60.6.39",8500,"us-option-kafka"));
        System.out.println(getServiceLists("10.60.6.39",8500,"yxzq-hqyw-kafka"));
        System.out.println(getServiceLists("10.60.6.39", 8500, "quotes-dataservice"));
        System.out.println(getSingleService("10.60.6.39", 8500, "quotes-dataservice"));
    }
    
}
