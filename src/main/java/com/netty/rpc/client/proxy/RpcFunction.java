package com.netty.rpc.client.proxy;

/**
 * lambda method reference
 * g-yu
 */
@FunctionalInterface
public interface RpcFunction<T, P> extends SerializableFunction<T> {
    Object apply(T t, P p);
}
