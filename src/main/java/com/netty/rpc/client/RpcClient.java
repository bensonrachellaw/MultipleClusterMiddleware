package com.netty.rpc.client;

import com.netty.rpc.common.annotation.RpcAutowired;
import com.netty.rpc.client.connect.ConnectionManager;
import com.netty.rpc.client.discovery.ServiceDiscovery;
import com.netty.rpc.client.proxy.ObjectProxy;
import com.netty.rpc.client.proxy.RpcService;
import com.netty.rpc.common.protocol.RpcProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * RPC Client（Create RPC proxy）
 *
 * @author luxiaoxun
 * @author g-yu
 */
// 初始化service channel 和 注入类属性对象
public class RpcClient implements ApplicationContextAware, DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(RpcClient.class);

    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(16, 16,
            600L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1000));

    // 初始化
    public RpcClient(String address) {
        // TODO:这里指定是否使用zk
        ServiceDiscovery serviceDiscovery = new ServiceDiscovery(address);

    }



    @SuppressWarnings("unchecked")
    // 返回proxy对象 用于初始化对象中属性带有对象的情况
    //TODO 指定isAssign 和 rpcProtocol
    public static <T, P> T createService(Class<T> interfaceClass, String version, Boolean isAssignProtocol, RpcProtocol rpcProtocol) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new ObjectProxy<T, P>(interfaceClass, version, isAssignProtocol, rpcProtocol)
        );
    }

    // TODO 指定requestId
    public static <T, P> T createService(Class<T> interfaceClass, String version, Boolean isAssignProtocol, RpcProtocol rpcProtocol,boolean isAssignId, String requestId) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new ObjectProxy<T, P>(interfaceClass, version, isAssignProtocol, rpcProtocol,isAssignId, requestId)
        );
    }

    public static <T, P> RpcService createAsyncService(Class<T> interfaceClass, String version) {
        return new ObjectProxy<T, P>(interfaceClass, version, false, null);
    }

    public static void submit(Runnable task) {
        threadPoolExecutor.submit(task);
    }

    public void stop() {
        threadPoolExecutor.shutdown();
        // TODO:关闭zk
//        serviceDiscovery.stop();
        ConnectionManager.getInstance().stop();
    }

    @Override
    public void destroy() throws Exception {
        this.stop();
    }

    @Override
    // 扫描包的时候顺带注入包内属性
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        // 得到bean对象名
        String[] beanNames = applicationContext.getBeanDefinitionNames();
        for (String beanName : beanNames) {
            Object bean = applicationContext.getBean(beanName);
            Field[] fields = bean.getClass().getDeclaredFields();
            try {
                for (Field field : fields) {
                    // 得到bean中属性对象需要实现类的版本
                    // 这个属性对应的非String Integer等包装类，而是自己实现的实现类对象
                    RpcAutowired rpcAutowired = field.getAnnotation(RpcAutowired.class);
                    if (rpcAutowired != null) {
                        String version = rpcAutowired.version();
                        field.setAccessible(true);
                        // 在这里进行实例化
                        field.set(bean, createService(field.getType(), version, false, null));
                    }
                }
            } catch (IllegalAccessException e) {
                logger.error(e.toString());
            }
        }
    }
}

