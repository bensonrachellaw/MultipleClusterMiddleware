package com.szubd.mcdfs_and_rspc.controller;

import com.netty.rpc.client.RpcClient;
import com.szubd.mcdfs_and_rspc.bean.Result;
import com.szubd.mcdfs_and_rspc.service.YarnService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;

@RequestMapping("/yarn")
@RestController
@Api("yarn管理")
public class YarnController {
    @Autowired
    private YarnService yarnService;


    @ApiOperation("根据appId获取yarn日志信息")
    @GetMapping("/getApp")
    @ApiImplicitParam(name = "appId",value = "yarnLog",dataType = "String")
    public Result ls(String appId) {
        String log = yarnService.getLog(appId);
        return new Result(log);
    }
}
