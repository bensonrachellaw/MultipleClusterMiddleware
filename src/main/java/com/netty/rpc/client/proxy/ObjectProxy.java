package com.netty.rpc.client.proxy;

import com.netty.rpc.client.connect.ConnectionManager;
import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.client.handler.RpcFuture;
import com.netty.rpc.common.codec.RpcRequest;
import com.netty.rpc.common.protocol.RpcProtocol;
import com.netty.rpc.common.util.ServiceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Created by luxiaoxun on 2016-03-16.
 */
// 在代理对象里面发送请求
public class ObjectProxy<T, P> implements InvocationHandler, RpcService<T, P, SerializableFunction<T>> {
    private static final Logger logger = LoggerFactory.getLogger(ObjectProxy.class);
    private Class<T> clazz;
    private String version;
    private Boolean isAssignProtocol = false;
    private RpcProtocol rpcProtocol = null;
    private boolean isAssignId = false;
    private String requestId = null;

    public ObjectProxy(Class<T> clazz, String version, Boolean isAssignProtocol, RpcProtocol rpcProtocol) {
        this.clazz = clazz;
        this.version = version;
        this.isAssignProtocol = isAssignProtocol;
        this.rpcProtocol = rpcProtocol;
    }
    public ObjectProxy(Class<T> clazz, String version, Boolean isAssignProtocol, RpcProtocol rpcProtocol, Boolean isAssignId, String requestId) {
        this.clazz = clazz;
        this.version = version;
        this.isAssignProtocol = isAssignProtocol;
        this.rpcProtocol = rpcProtocol;
        this.isAssignId = isAssignId;
        this.requestId = requestId;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (Object.class == method.getDeclaringClass()) {
            String name = method.getName();
            // 构造方法
            if ("equals".equals(name)) {
                return proxy == args[0];
            } else if ("hashCode".equals(name)) { //hashcode方法
                return System.identityHashCode(proxy);
            } else if ("toString".equals(name)) { //toString方法
                return proxy.getClass().getName() + "@" +
                        Integer.toHexString(System.identityHashCode(proxy)) +
                        ", with InvocationHandler " + this;
            } else {
                throw new IllegalStateException(String.valueOf(method));
            }
        }
        // 设置请求对象
        RpcRequest request = new RpcRequest();
        if(isAssignId) {
            request.setRequestId(this.requestId);
        }else {
            request.setRequestId(UUID.randomUUID().toString());
        }
        request.setClassName(method.getDeclaringClass().getName());
        request.setMethodName(method.getName());
        request.setParameterTypes(method.getParameterTypes());
        request.setParameters(args);
        request.setVersion(version);
        // Debug
        if (logger.isDebugEnabled()) {
            logger.debug(method.getDeclaringClass().getName());
            logger.debug(method.getName());
            for (int i = 0; i < method.getParameterTypes().length; ++i) {
                logger.debug(method.getParameterTypes()[i].getName());
            }
            if(args != null) {
                for (int i = 0; i < args.length; ++i) {
                    logger.debug(args[i].toString());
                }
            }
        }
        // 以一个接口的实现类对象名+版本号作为这次的serviceKey
        String serviceKey = ServiceUtil.makeServiceKey(method.getDeclaringClass().getName(), version);
        RpcFuture rpcFuture = null;
        if(!isAssignProtocol) {
            // 选择一个合适的Client任务处理器 因为handler中绑定了channel
            RpcClientHandler handler = ConnectionManager.getInstance().chooseHandler(serviceKey);
            // 在这里可以选择调用所有handler或者通过负载均衡单个handler
//        List<RpcClientHandler> handlers = ConnectionManager.getInstance().allUseFulHandle(serviceKey);
            // 发送rpc请求,并等待回调
            rpcFuture = handler.sendRequest(request); // 在send的同时会将rpcFuture加入到pending map中
//        CountDownLatch countDownLatch = new CountDownLatch(handlers.size());
//        for (RpcClientHandler handler : handlers) {
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    RpcFuture rpcFuture = handler.sendRequest(request);
//                    Object o = rpcFuture.get();
//                    countDownLatch.countDown();
//                }
//            }).start();
//        }
//        countDownLatch.await();
        }else {
            // TODO:直接返回handler 不用校验
            RpcClientHandler handler = ConnectionManager.getInstance().assignHandler(serviceKey, this.rpcProtocol);
            rpcFuture = handler.sendRequest(request);
        }
        System.out.println("发送请求成功");
//        return rpcFuture.get(); // 这里阻塞住进行等待回调结果 如果还未返回则返回null
        return rpcFuture.get(50000, TimeUnit.SECONDS);
    }

    // 调用接口
    @Override
    public RpcFuture call(String funcName, Object... args) throws Exception {
        String serviceKey = ServiceUtil.makeServiceKey(this.clazz.getName(), version);
        RpcClientHandler handler = ConnectionManager.getInstance().chooseHandler(serviceKey);
        RpcRequest request = createRequest(this.clazz.getName(), funcName, args);
        RpcFuture rpcFuture = handler.sendRequest(request);
        return rpcFuture;
    }


    @Override
    public RpcFuture call(SerializableFunction<T> tSerializableFunction, Object... args) throws Exception {
        String serviceKey = ServiceUtil.makeServiceKey(this.clazz.getName(), version);
        // 负载均衡，找服务端channel
        RpcClientHandler handler = ConnectionManager.getInstance().chooseHandler(serviceKey);
        RpcRequest request = createRequest(this.clazz.getName(), tSerializableFunction.getName(), args);
        RpcFuture rpcFuture = handler.sendRequest(request);
        return rpcFuture;
    }

    private RpcRequest createRequest(String className, String methodName, Object[] args) {
        RpcRequest request = new RpcRequest();
        request.setRequestId(UUID.randomUUID().toString());
        request.setClassName(className);
        request.setMethodName(methodName);
        request.setParameters(args);
        request.setVersion(version);
        Class[] parameterTypes = new Class[args.length];
        // Get the right class type
        for (int i = 0; i < args.length; i++) {
            parameterTypes[i] = getClassType(args[i]);
        }
        request.setParameterTypes(parameterTypes);

        // Debug
        if (logger.isDebugEnabled()) {
            logger.debug(className);
            logger.debug(methodName);
            for (int i = 0; i < parameterTypes.length; ++i) {
                logger.debug(parameterTypes[i].getName());
            }
            for (int i = 0; i < args.length; ++i) {
                logger.debug(args[i].toString());
            }
        }

        return request;
    }

    private Class<?> getClassType(Object obj) {
        Class<?> classType = obj.getClass();
//        String typeName = classType.getName();
//        switch (typeName) {
//            case "java.lang.Integer":
//                return Integer.TYPE;
//            case "java.lang.Long":
//                return Long.TYPE;
//            case "java.lang.Float":
//                return Float.TYPE;
//            case "java.lang.Double":
//                return Double.TYPE;
//            case "java.lang.Character":
//                return Character.TYPE;
//            case "java.lang.Boolean":
//                return Boolean.TYPE;
//            case "java.lang.Short":
//                return Short.TYPE;
//            case "java.lang.Byte":
//                return Byte.TYPE;
//        }
        return classType;
    }

}
