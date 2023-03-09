package com.netty.rpc.client.route.impl;

import com.google.common.hash.Hashing;
import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.client.route.RpcLoadBalance;
import com.netty.rpc.common.protocol.RpcProtocol;

import java.util.List;
import java.util.Map;

public class RpcLoadBalanceConsistentHash extends RpcLoadBalance {

    public RpcProtocol doRoute(String serviceKey, List<RpcProtocol> addressList) {
        int index = Hashing.consistentHash(serviceKey.hashCode(), addressList.size());
        return addressList.get(index);
    }

    @Override
    public RpcProtocol route(String serviceKey, Map<RpcProtocol, RpcClientHandler> connectedServerNodes) throws Exception {
        Map<String, List<RpcProtocol>> serviceMap = getServiceMap(connectedServerNodes);
        List<RpcProtocol> addressList = serviceMap.get(serviceKey);
        if (addressList != null && addressList.size() > 0) {
            return doRoute(serviceKey, addressList);
        } else {
            throw new Exception("Can not find connection for service: " + serviceKey);
        }
    }
}
