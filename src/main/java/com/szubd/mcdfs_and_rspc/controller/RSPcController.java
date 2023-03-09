package com.szubd.mcdfs_and_rspc.controller;
import com.szubd.mcdfs_and_rspc.bean.Result;
import com.szubd.mcdfs_and_rspc.bean.SparkJob;
import com.szubd.mcdfs_and_rspc.bean.SparkJobWithUrl;
import com.szubd.mcdfs_and_rspc.eum.SparkJobState;
import com.szubd.mcdfs_and_rspc.service.RSPcService;
import com.szubd.mcdfs_and_rspc.webSocket.RealTimeRSPcWebSocket;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import java.util.*;


@RequestMapping("/rspc")
@RestController
@Api("spark远程调用")
public class RSPcController {
    @Value("#{'${livy.url}'.split(',')}")
    private List<String> urls;
    @Value("${hadoop.user}")
    private String user;

    @Autowired
    private BeanFactory beanFactory;

    private Map<String, RSPcService> rsPcServiceMap;
    private Map<RSPcService, String> urlMap;

    private Logger logger = LogManager.getLogger(this.getClass());

    @PostConstruct
    // 初始化所有service 根据ip和service组成map 多例！
    public void init() {
        Map<String, RSPcService> map1 = new HashMap<>();
        Map<RSPcService, String> map2 = new HashMap<>();
        for (String url : urls) {
            RSPcService rsPcService = beanFactory.getBean(RSPcService.class);
            rsPcService.initUrl(url);
            map1.put(url, rsPcService);
            map2.put(rsPcService, url);
        }
        this.rsPcServiceMap = map1;
        this.urlMap = map2;
    }


     //对于所有集群执行同一个job
//    {
//      "className": "org.apache.spark.examples.SparkPi",
//      "file": "local:/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/examples/jars/spark-examples_2.11-2.4.0-cdh6.3.2.jar",
//      "name": "Testing_kity_70",
//      "proxyUser": "luokaijing"
//    }
    @ApiOperation("提交spark任务")
    @PostMapping("/submit")
    @ApiImplicitParam(name = "sparkJob",value = "sparkJob参数",paramType = "body",dataType = "SparkJob")
    public Result submit(@RequestBody SparkJob sparkJob) {
        Collection<RSPcService> rsPcServices = this.rsPcServiceMap.values();
        Map<String, Integer> result = new HashMap<>();
        for (RSPcService rsPcService : rsPcServices) {
            try {
                int id = rsPcService.startSparkJob(sparkJob);
                result.put(urlMap.get(rsPcService), id);
            } catch (Exception e) {
                logger.error("任务提交失败", e);
            }
        }
        return new Result(result);
    }

    // 对某个集群的某个id任务查询状态
    @ApiOperation("查询spark任务状态")
    @PostMapping("/getState")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "id",value = "提交任务id",dataType = "Integer"),
        @ApiImplicitParam(name = "url",value = "spark集群路径",dataType = "String")
    })
    public Result getState(Integer id, String url){
        RSPcService rsPcService = rsPcServiceMap.get(url);
        SparkJobState sparkJobState = rsPcService.getSparkJobState(id);
        if(sparkJobState == null){
            return new Result("查无此任务");
        }
        return new Result(sparkJobState);
    }

    // 对某个集群的某个id任务查询信息
    @ApiOperation("查询spark任务信息")
    @GetMapping("/getInfo")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id",value = "提交任务id",dataType = "Integer"),
            @ApiImplicitParam(name = "url",value = "spark集群路径",dataType = "String")
    })
    public Result getSparkJobInfo(Integer id, String url){
        RSPcService rsPcService = rsPcServiceMap.get(url);
        Map<String, Object> info = rsPcService.getSparkJobInfo(id);
        if(info.size() <= 1){
            return new Result(info.get("msg"));
        }
        Map appInfo = (Map) info.get("appInfo");
        Map<String, String> infoMap = new HashMap<>();
        infoMap.put("batchId",id.toString());
        infoMap.put("appId",(String) info.get("appId"));
        infoMap.put("name",(String)info.get("name"));
//        infoMap.put("proxyUser",(String)info.get("proxyUser"));
        infoMap.put("state",(String) info.get("state"));
        if(appInfo.get("driverLogUrl") != null && !appInfo.get("driverLogUrl").equals("null")){
            infoMap.put("driver",(String) appInfo.get("driverLogUrl") );
        }
        if(appInfo.get("driverLogUrl") != null && !appInfo.get("sparkUiUrl").equals("null")){
            infoMap.put("sparkUI",(String) appInfo.get("sparkUiUrl"));
        }
        return new Result(infoMap);
    }
//
//    @ApiOperation("提交并实时监控sparkSubmit")
//    @GetMapping("/realTimeSubmit")
//    @ApiImplicitParam(name = "sparkJob",value = "sparkJob参数",paramType = "body",dataType = "SparkJob")
//    public Result getState(@RequestBody SparkJob sparkJob){
//        // 首先要保证有人连接
//        if(RealTimeRSPcWebSocket.webSockets.isEmpty()) return new Result("无实时窗口");
//        // TODO:多线程
//        boolean flag = rsPcService.runSparkJob(sparkJob);
//        if(flag) return new Result("执行成功");
//        return new Result("执行失败");
//    }
}
