package com.netty.rpc.client.connect;

import com.netty.rpc.client.handler.RpcClientHandler;
import com.netty.rpc.client.handler.RpcClientInitializer;
import com.netty.rpc.client.route.RpcLoadBalance;
import com.netty.rpc.client.route.impl.RpcAllLoad;
import com.netty.rpc.client.route.impl.RpcLoadBalanceRoundRobin;
import com.netty.rpc.common.protocol.RpcProtocol;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * RPC Connection Manager
 * Created by luxiaoxun on 2016-03-16.
 */
public class ConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    private EventLoopGroup eventLoopGroup = new NioEventLoopGroup(4);
    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(4, 8,
            600L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1000));
    // 需要维护的关系 : ServiceInfo - RpcProtocol - RpcClientHandler
    // 确定唯一关系 每个客户端只有一个channel
    private Map<RpcProtocol, RpcClientHandler> connectedServerNodes = new ConcurrentHashMap<>();
    public CopyOnWriteArraySet<RpcProtocol> rpcProtocolSet = new CopyOnWriteArraySet<>(); // 线程安全 存储着所有服务器的ip信息

    private ReentrantLock lock = new ReentrantLock();
    private Condition connected = lock.newCondition();
    private long waitTimeout = 5000;
    private RpcLoadBalance loadBalance = new RpcLoadBalanceRoundRobin();
    private RpcAllLoad loadAll = new RpcAllLoad();
    private volatile boolean isRunning = true;

    private ConnectionManager() {
    }

    // 单例
    private static class SingletonHolder {
        private static final ConnectionManager instance = new ConnectionManager();
    }

    public static ConnectionManager getInstance() {
        return SingletonHolder.instance;
    }

    public void orderRpcProtocolConnectedServerWithoutServiceInfoList(List<RpcProtocol> serviceList){
        if (serviceList != null && serviceList.size() > 0) {
            for (int i = 0; i < serviceList.size(); ++i) {
                RpcProtocol rpcProtocol = serviceList.get(i);
                if (!rpcProtocolSet.contains(rpcProtocol)) {
                    connectServerNode(rpcProtocol);
                }
            }
        }else {
            // No available service
            logger.error("No available service!");
//            for (RpcProtocol rpcProtocol : rpcProtocolSet) {
//                removeAndCloseHandler(rpcProtocol);
//            }
        }
    }

    public void updateConnectedServer(List<RpcProtocol> serviceList) {
        // Now using 2 collections to manage the service info and TCP connections because making the connection is async
        // Once service info is updated on ZK, will trigger this function
        // Actually client should only care about the service it is using
        if (serviceList != null && serviceList.size() > 0) {
            // Update local server nodes cache
            HashSet<RpcProtocol> serviceSet = new HashSet<>(serviceList.size());
            for (int i = 0; i < serviceList.size(); ++i) {
                RpcProtocol rpcProtocol = serviceList.get(i);
                serviceSet.add(rpcProtocol);
            }

            // Add new server info
            // 加入新的服务端channel
            for (final RpcProtocol rpcProtocol : serviceSet) {
                if (!rpcProtocolSet.contains(rpcProtocol)) {
                    connectServerNode(rpcProtocol);
                }
            }

            // Close and remove invalid server nodes
            for (RpcProtocol rpcProtocol : rpcProtocolSet) {
                if (!serviceSet.contains(rpcProtocol)) {
                    logger.info("Remove invalid service: " + rpcProtocol.toJson());
                    removeAndCloseHandler(rpcProtocol);
                }
            }
        } else {
            // No available service
            logger.error("No available service!");
            for (RpcProtocol rpcProtocol : rpcProtocolSet) {
                removeAndCloseHandler(rpcProtocol);
            }
        }
    }


    public void updateConnectedServer(RpcProtocol rpcProtocol, PathChildrenCacheEvent.Type type) {
        if (rpcProtocol == null) {
            return;
        }
        if (type == PathChildrenCacheEvent.Type.CHILD_ADDED && !rpcProtocolSet.contains(rpcProtocol)) {
            connectServerNode(rpcProtocol);
        } else if (type == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
            //TODO We may don't need to reconnect remote server if the server'IP and server'port are not changed
            removeAndCloseHandler(rpcProtocol);
            connectServerNode(rpcProtocol);
        } else if (type == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
            removeAndCloseHandler(rpcProtocol);
        } else {
            throw new IllegalArgumentException("Unknow type:" + type);
        }
    }

    // 建立channel 初始化容器的时候会调用到
    private void connectServerNode(RpcProtocol rpcProtocol) {
//        if (rpcProtocol.getServiceInfoList() == null || rpcProtocol.getServiceInfoList().isEmpty()) {
//            logger.info("No service on node, host: {}, port: {}", rpcProtocol.getHost(), rpcProtocol.getPort());
//            return;
//        }
        // 线程安全的set加入Protocol
        rpcProtocolSet.add(rpcProtocol);
        logger.info("New service node, host: {}, port: {}", rpcProtocol.getHost(), rpcProtocol.getPort());
        // 获取服务端的serviceInfo，即可调用的服务
//        for (RpcServiceInfo serviceProtocol : rpcProtocol.getServiceInfoList()) {
//            logger.info("New service info, name: {}, version: {}", serviceProtocol.getServiceName(), serviceProtocol.getVersion());
//        }
        // 得到新的服务的ip端口号对象
        // TODO:这里和指定ip端口号进行连接
        final InetSocketAddress remotePeer = new InetSocketAddress(rpcProtocol.getHost(), rpcProtocol.getPort());
        // 开启新的线程与新的服务建立channel
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                Bootstrap b = new Bootstrap();
                b.group(eventLoopGroup)
                        .channel(NioSocketChannel.class)
                        .handler(new RpcClientInitializer());

                ChannelFuture channelFuture = b.connect(remotePeer);
                // 连接成功后
                channelFuture.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(final ChannelFuture channelFuture) throws Exception {
                        if (channelFuture.isSuccess()) {
                            logger.info("Successfully connect to remote server, remote peer = " + remotePeer);
                            // 拿到这个channel的RpcClientHandler
                            RpcClientHandler handler = channelFuture.channel().pipeline().get(RpcClientHandler.class);
                            // 将改channel的RpcClientHandler和rpcProtocol信息绑定成map元素
                            connectedServerNodes.put(rpcProtocol, handler);
                            // 设置改handler归属
                            handler.setRpcProtocol(rpcProtocol);
                            // 唤醒所有wait的线程
                            signalAvailableHandler();
                        } else {
                            logger.error("Can not connect to remote server, remote peer = " + remotePeer);
                        }
                    }
                });
            }
        });
    }

    // 唤醒AQS队列里的所有线程
    private void signalAvailableHandler() {
        lock.lock();
        try {
            connected.signalAll();
        } finally {
            lock.unlock();
        }
    }

    // 加入AQS队列等待唤醒
    private boolean waitingForHandler() throws InterruptedException {
        lock.lock();
        try {
            logger.warn("Waiting for available service");
            return connected.await(this.waitTimeout, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    //返回所有拥有该服务的服务端
    public List<RpcClientHandler> allUseFulHandle(String serviceKey) throws Exception{
        int size = connectedServerNodes.values().size();
        while (isRunning && size <= 0) { // 如果当前没有服务可以用，则需要等待唤醒
            try {
                waitingForHandler();
                size = connectedServerNodes.values().size();
            } catch (InterruptedException e) {
                logger.error("Waiting for available service is interrupted!", e);
            }
        }
        List<RpcProtocol> rpcProtocols = loadAll.route(serviceKey, connectedServerNodes);
        List<RpcClientHandler> handlers = new ArrayList<>();
        for (RpcProtocol rpcProtocol : rpcProtocols) {
            handlers.add(connectedServerNodes.get(rpcProtocol));
        }
        if(handlers.size() > 0) return handlers;
        else throw new Exception("Can not get available connection");
    }

    // 指定ip选择服务端 前提是知道该端口具有该服务
    public RpcClientHandler assignHandler(String serviceKey, RpcProtocol rpcProtocol) throws Exception{
        int size = connectedServerNodes.values().size();
        while (isRunning && size <= 0) { // 如果当前没有服务可以用，则需要等待唤醒
            try {
                waitingForHandler();
                size = connectedServerNodes.values().size();
            } catch (InterruptedException e) {
                logger.error("Waiting for available service is interrupted!", e);
            }
        }
        return connectedServerNodes.get(rpcProtocol);

    }

    // 算法选择服务端 调用代理对象时会调用
    public RpcClientHandler chooseHandler(String serviceKey) throws Exception {
        int size = connectedServerNodes.values().size();
        while (isRunning && size <= 0) { // 如果当前没有服务可以用，则需要等待唤醒
            try {
                waitingForHandler();
                size = connectedServerNodes.values().size();
            } catch (InterruptedException e) {
                logger.error("Waiting for available service is interrupted!", e);
            }
        }
        RpcProtocol rpcProtocol = loadBalance.route(serviceKey, connectedServerNodes);// 得到服务端
        RpcClientHandler handler = connectedServerNodes.get(rpcProtocol); // 查询得到服务端对应的RpcClientHandler
        if (handler != null) {
            return handler;
        } else {
            throw new Exception("Can not get available connection");
        }
    }

    private void removeAndCloseHandler(RpcProtocol rpcProtocol) {
        RpcClientHandler handler = connectedServerNodes.get(rpcProtocol);
        if (handler != null) {
            handler.close();
        }
        connectedServerNodes.remove(rpcProtocol);
        rpcProtocolSet.remove(rpcProtocol);
    }

    public void removeHandler(RpcProtocol rpcProtocol) {
        rpcProtocolSet.remove(rpcProtocol);
        connectedServerNodes.remove(rpcProtocol);
        logger.info("Remove one connection, host: {}, port: {}", rpcProtocol.getHost(), rpcProtocol.getPort());
    }

    public void stop() {
        isRunning = false;
        for (RpcProtocol rpcProtocol : rpcProtocolSet) {
            removeAndCloseHandler(rpcProtocol);
        }
        signalAvailableHandler();
        threadPoolExecutor.shutdown();
        eventLoopGroup.shutdownGracefully();
    }
}
