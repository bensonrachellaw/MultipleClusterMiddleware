package com.netty.rpc;

import com.netty.rpc.client.RpcClient;
import com.netty.rpc.client.connect.ConnectionManager;
import com.netty.rpc.common.protocol.RpcProtocol;
import com.app.test.service.HelloService;

import java.util.concurrent.CopyOnWriteArraySet;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        final RpcClient rpcClient = new RpcClient("127.0.0.1:2181");

        int threadNum = 1;
        final int requestNum = 50;
        Thread[] threads = new Thread[threadNum];

        long startTime = System.currentTimeMillis();
        //benchmark for sync call
        CopyOnWriteArraySet<RpcProtocol> rpcProtocolSet = ConnectionManager.getInstance().rpcProtocolSet;

        for (RpcProtocol rpcProtocol : rpcProtocolSet) {
            System.out.println(rpcProtocol);
        }
//        for (int i = 0; i < threadNum; ++i) {
//            threads[i] = new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    for (int i = 0; i < requestNum; i++) {
//                        try {
//                            // 可以在这里入手 create创建每个ip对应的代理对象，传入参数需要加一个ip，然后通过ip得到channel，不用choose，自己指定一个指定ip获取channel方法
//                            final HelloService syncClient = rpcClient.createService(HelloService.class, "1.0", true,  rpcProtocolSet.iterator().next());
//                            String result = syncClient.hello(Integer.toString(i));
//                            System.out.println("result = " + result);
//                            try {
//                                Thread.sleep(5 * 1000);
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                        } catch (Exception ex) {
//                            //当服务端都关闭会报错  java.util.NoSuchElementException
////                            System.out.println(ex.toString());
//                        }
//                    }
//                }
//            });
//            threads[i].start();
//        }
        final HelloService syncClient = rpcClient.createService(HelloService.class, "1.0", true,  rpcProtocolSet.iterator().next());
        String result = syncClient.hello(Integer.toString(1));
        System.out.println("result = " + result);
//        for (int i = 0; i < threads.length; i++) {
//            threads[i].join();
//        }
        long timeCost = (System.currentTimeMillis() - startTime);
        String msg = String.format("Sync call total-time-cost:%sms, req/s=%s", timeCost, ((double) (requestNum * threadNum)) / timeCost * 1000);
        System.out.println(msg);

        rpcClient.stop();
    }
}
