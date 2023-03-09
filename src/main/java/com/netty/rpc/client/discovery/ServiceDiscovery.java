package com.netty.rpc.client.discovery;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.netty.rpc.client.connect.ConnectionManager;
import com.netty.rpc.common.config.Constant;
import com.netty.rpc.common.protocol.RpcProtocol;
import com.netty.rpc.common.zookeeper.CuratorClient;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 服务发现
 *
 * @author luxiaoxun
 */
// 被注入到spring容器中
public class ServiceDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(ServiceDiscovery.class);
    private CuratorClient curatorClient;

    public ServiceDiscovery(String registryAddress) {
//        this.curatorClient = new CuratorClient(registryAddress);
        // service进行更新和连接
//        discoveryService();
        ConnectService();
    }

    private void ConnectService(){
        getServiceByProperties();
    }

    private void discoveryService() {
        try {
            // Get initial service info
            logger.info("Get initial service info");
            // 在这里加载所有服务器的ip信息
            getServiceAndUpdateServer();
            // Add watch listener
            // 对于service进行监听，如果有更改即更新connectedServerNodes map集合,即内存中的数据
            curatorClient.watchPathChildrenNode(Constant.ZK_REGISTRY_PATH, new PathChildrenCacheListener() {
                @Override
                public void childEvent (CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                    PathChildrenCacheEvent.Type type = pathChildrenCacheEvent.getType();
                    ChildData childData = pathChildrenCacheEvent.getData();
                    switch (type) {
                        case CONNECTION_RECONNECTED:
                            logger.info("Reconnected to zk, try to get latest service list");
                            getServiceAndUpdateServer();
                            break;
                        case CHILD_ADDED:
                            getServiceAndUpdateServer(childData, PathChildrenCacheEvent.Type.CHILD_ADDED);
                            break;
                        case CHILD_UPDATED:
                            getServiceAndUpdateServer(childData, PathChildrenCacheEvent.Type.CHILD_UPDATED);
                            break;
                        case CHILD_REMOVED:
                            getServiceAndUpdateServer(childData, PathChildrenCacheEvent.Type.CHILD_REMOVED);
                            break;
                    }
                }
            });
        } catch (Exception ex) {
            logger.error("Watch node exception: " + ex.getMessage());
        }
    }

    private void getServiceByProperties(){
        File file = new File("C:\\Users\\Amo\\Desktop\\NettyRpc-master\\netty-rpc-test\\src\\main\\resources\\service.json");
        try {
            String fileString = FileUtils.readFileToString(file, "UTF-8");
            List<RpcProtocol> rpcProtocols = JSON.parseObject(fileString, new TypeReference<List<RpcProtocol>>() {
            });
            System.out.println(rpcProtocols);
            ConnectionManager.getInstance().orderRpcProtocolConnectedServerWithoutServiceInfoList(rpcProtocols);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getServiceAndUpdateServer() {
        try {
            // 获取所有的service节点
            List<String> nodeList = curatorClient.getChildren(Constant.ZK_REGISTRY_PATH);
            List<RpcProtocol> dataList = new ArrayList<>();
            for (String node : nodeList) {
                logger.debug("Service node: " + node);
                // 获取service节点下的数据
                byte[] bytes = curatorClient.getData(Constant.ZK_REGISTRY_PATH + "/" + node);
                String json = new String(bytes);
                RpcProtocol rpcProtocol = RpcProtocol.fromJson(json);
                dataList.add(rpcProtocol);
            }
            System.out.println(dataList);
            logger.debug("Service node data: {}", dataList);
            //Update the service info based on the latest data
            // 更新
            UpdateConnectedServer(dataList);
        } catch (Exception e) {
            logger.error("Get node exception: " + e.getMessage());
        }
    }

    private void getServiceAndUpdateServer(ChildData childData, PathChildrenCacheEvent.Type type) {
        String path = childData.getPath();
        String data = new String(childData.getData(), StandardCharsets.UTF_8);
        logger.info("Child data updated, path:{},type:{},data:{},", path, type, data);
        RpcProtocol rpcProtocol =  RpcProtocol.fromJson(data);
        updateConnectedServer(rpcProtocol, type);
    }

    private void UpdateConnectedServer(List<RpcProtocol> dataList) {
        ConnectionManager.getInstance().updateConnectedServer(dataList);
    }


    private void updateConnectedServer(RpcProtocol rpcProtocol, PathChildrenCacheEvent.Type type) {
        ConnectionManager.getInstance().updateConnectedServer(rpcProtocol, type);
    }

    public void stop() {
        this.curatorClient.close();
    }
}
