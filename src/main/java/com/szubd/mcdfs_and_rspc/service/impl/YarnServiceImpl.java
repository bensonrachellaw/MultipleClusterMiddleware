package com.szubd.mcdfs_and_rspc.service.impl;

import com.netty.rpc.client.RpcClient;
import com.netty.rpc.client.connect.ConnectionManager;
import com.netty.rpc.common.protocol.RpcProtocol;
import com.app.test.service.NettyYarnService;
import com.szubd.mcdfs_and_rspc.service.YarnService;
import org.springframework.stereotype.Service;

import java.util.concurrent.CopyOnWriteArraySet;

import static com.szubd.mcdfs_and_rspc.RpcInitializer.rpcClient;

@Service
public class YarnServiceImpl implements YarnService {
    @Override
    public String getLog(String appId) {
        // 这里初始化就已经建立好连接
        // 如果stop就会销毁成员变量的线程池，线程池是静态变量，全局只有一份，再次初始化就会报错
        CopyOnWriteArraySet<RpcProtocol> rpcProtocolSet = ConnectionManager.getInstance().rpcProtocolSet;

        // 暂定取第一个获取log 后面根据ip地址
        final NettyYarnService syncClient = rpcClient.createService(NettyYarnService.class, "", true,  rpcProtocolSet.iterator().next());
        String result = null;
        try {
            result = syncClient.getAppLog(appId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
