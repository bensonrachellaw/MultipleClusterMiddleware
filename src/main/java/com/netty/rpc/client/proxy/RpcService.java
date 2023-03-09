package com.netty.rpc.client.proxy;

import com.netty.rpc.client.handler.RpcFuture;

/**
 * Created by luxiaoxun on 2016/3/16.
 *
 * @author g-yu
 */
public interface RpcService<T, P, FN extends SerializableFunction<T>> {
    RpcFuture call(String funcName, Object... args) throws Exception;

    /**
     * lambda method reference
     */
    RpcFuture call(FN fn, Object... args) throws Exception;

}