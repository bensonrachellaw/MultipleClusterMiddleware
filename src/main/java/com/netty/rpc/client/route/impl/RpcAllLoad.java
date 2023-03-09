package com.netty.rpc.client.route.impl;

import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.common.protocol.RpcProtocol;
import com.netty.rpc.common.protocol.RpcServiceInfo;
import com.netty.rpc.common.util.ServiceUtil;
import org.apache.commons.collections4.map.HashedMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RpcAllLoad {
    protected Map<String, List<RpcProtocol>> getServiceMap(Map<RpcProtocol, RpcClientHandler> connectedServerNodes) {
        Map<String, List<RpcProtocol>> serviceMap = new HashedMap<>();
        if (connectedServerNodes != null && connectedServerNodes.size() > 0) {
            for (RpcProtocol rpcProtocol : connectedServerNodes.keySet()) {
                for (RpcServiceInfo serviceInfo : rpcProtocol.getServiceInfoList()) {
                    String serviceKey = ServiceUtil.makeServiceKey(serviceInfo.getServiceName(), serviceInfo.getVersion());
                    List<RpcProtocol> rpcProtocolList = serviceMap.get(serviceKey);
                    if (rpcProtocolList == null) {
                        rpcProtocolList = new ArrayList<>();
                    }
                    rpcProtocolList.add(rpcProtocol);
                    serviceMap.putIfAbsent(serviceKey, rpcProtocolList);
                }
            }
        }
        return serviceMap;
    }

    public  List<RpcProtocol> route(String serviceKey, Map<RpcProtocol, RpcClientHandler> connectedServerNodes) throws Exception{
        Map<String, List<RpcProtocol>> serviceMap = getServiceMap(connectedServerNodes);
        List<RpcProtocol> addressList = serviceMap.get(serviceKey); // 通过需要调用的服务来查寻拥有该服务的服务端
        if (addressList != null && addressList.size() > 0) {
            return addressList;// 返回所有服务端
        } else {
            throw new Exception("Can not find connection for service: " + serviceKey);
        }
    }

}
