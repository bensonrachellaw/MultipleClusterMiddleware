package com.netty.rpc.client.route.impl;

import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.client.route.RpcLoadBalance;
import com.netty.rpc.common.protocol.RpcProtocol;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round robin load balance
 * Created by luxiaoxun on 2020-08-01.
 */
public class RpcLoadBalanceRoundRobin extends RpcLoadBalance {
    private AtomicInteger roundRobin = new AtomicInteger(0);

    // 采用轮询方法得到访问的服务端
    public RpcProtocol doRoute(List<RpcProtocol> addressList) {
        int size = addressList.size();
        // Round robin
        int index = (roundRobin.getAndAdd(1) + size) % size;
        return addressList.get(index);
    }

    @Override
    public RpcProtocol route(String serviceKey, Map<RpcProtocol, RpcClientHandler> connectedServerNodes) throws Exception {
        Map<String, List<RpcProtocol>> serviceMap = getServiceMap(connectedServerNodes);
        List<RpcProtocol> addressList = serviceMap.get(serviceKey); // 通过需要调用的服务来查寻拥有该服务的服务端
        if (addressList != null && addressList.size() > 0) {
            return doRoute(addressList);
        } else {
            throw new Exception("Can not find connection for service: " + serviceKey);
        }
    }
}
