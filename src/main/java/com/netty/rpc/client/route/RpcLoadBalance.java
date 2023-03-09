package com.netty.rpc.client.route;

import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.common.protocol.RpcProtocol;
import com.netty.rpc.common.protocol.RpcServiceInfo;
import com.netty.rpc.common.util.ServiceUtil;
import org.apache.commons.collections4.map.HashedMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by luxiaoxun on 2020-08-01.
 */
public abstract class RpcLoadBalance {
    // Service map: group by service name
    // 建立 service - RpcProtocol_list map
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

    // Route the connection for service key
    public abstract RpcProtocol route(String serviceKey, Map<RpcProtocol, RpcClientHandler> connectedServerNodes) throws Exception;
}
