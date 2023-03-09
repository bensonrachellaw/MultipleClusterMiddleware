package com.szubd.mcdfs_and_rspc;

import com.netty.rpc.client.RpcClient;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
public class RpcInitializer implements InitializingBean  {
    public static RpcClient rpcClient;
    @Override
    public void afterPropertiesSet() throws Exception {
        rpcClient = new RpcClient("127.0.0.1:2181");
    }
}
