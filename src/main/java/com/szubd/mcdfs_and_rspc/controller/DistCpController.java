package com.szubd.mcdfs_and_rspc.controller;

import com.szubd.mcdfs_and_rspc.bean.ClusterFileUploadInfo;
import com.szubd.mcdfs_and_rspc.bean.DistCpInfo;
import com.szubd.mcdfs_and_rspc.bean.Result;
import com.szubd.mcdfs_and_rspc.service.DistCpService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RequestMapping("/distCp")
@RestController
@Api("rsp数据块传输")
public class DistCpController {

    @Autowired
    private DistCpService distCpService;

    @ApiOperation("通过源集群和目标集群的hdfs路径来传递hdfs文件")
    @PostMapping("/hdfsTransfer")
    @ApiImplicitParam(name = "distCpInfo",paramType = "body",value = "源集群和目标集群的hdfs路径",dataType ="DistCpInfo")
    public Result hdfsTransfer(@RequestBody DistCpInfo distCpInfo) {
        int flag = 1;
        try {
            flag = distCpService.distcp(distCpInfo.getSrcHdfsPath(), distCpInfo.getDestHdfsPath());
        }catch (Exception e){
            return new Result(e.getMessage());
        }
        if(flag == 0)return new Result("传输成功！");
        return new Result("传输错误，请检查源地址是否存在或检查服务器日志");
    }
}
