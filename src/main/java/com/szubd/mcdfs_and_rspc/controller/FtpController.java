package com.szubd.mcdfs_and_rspc.controller;

import com.szubd.mcdfs_and_rspc.bean.ClusterFileDownloadInfo;
import com.szubd.mcdfs_and_rspc.bean.ClusterFileUploadInfo;
import com.szubd.mcdfs_and_rspc.bean.Result;
import com.szubd.mcdfs_and_rspc.service.FtpService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RequestMapping("/ftp")
@RestController
@Api("文件传输（非HDFS）")
public class FtpController {
    @Value("#{'${ftp.url}'.split(',')}")
    private List<String> urls;
    @Value("${ftp.username}")
    private String username;
    @Value("${ftp.password}")
    private String password;

    @Autowired
    private FtpService ftpService;

//    [
//      {
//        "basePath": "/home",
//        "filePath": "/zhangyuming",
//        "localFileName": "FTP.txt",
//        "localPath": "C:/Users/Amo/Desktop/FTP.txt",
//        "url": "172.31.238.102"
//      }
//    ]
    @ApiOperation("通过url和路径向每个集群发送upload请求，发送本地的某个数据文件")
    @PostMapping("/putByUrlAndPath")
    @ApiImplicitParam(name = "clusterFileUploadInfoList",paramType = "body",value = "集群url和集群路径",dataType ="ClusterFileUploadInfo",allowMultiple = true)
    public Result putByUrlAndPath(@RequestBody List<ClusterFileUploadInfo> clusterFileUploadInfoList) {
        CountDownLatch count = new CountDownLatch(clusterFileUploadInfoList.size());
        ExecutorService taskExecutor = Executors.newFixedThreadPool(clusterFileUploadInfoList.size());
        for (ClusterFileUploadInfo clusterFileUploadInfo : clusterFileUploadInfoList) {
            taskExecutor.execute(() -> {
                // String host, int port, String username, String password, String basePath,
                //                       String filePath, String filename, InputStream input
                try {
                    ftpService.uploadFile(clusterFileUploadInfo.getUrl(), 21, username, password, clusterFileUploadInfo.getBasePath(), clusterFileUploadInfo.getFilePath(), clusterFileUploadInfo.getLocalFileName(),
                            new FileInputStream(clusterFileUploadInfo.getLocalPath()));
                    count.countDown();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            });
        }

        taskExecutor.shutdown();
        try {
            boolean b = taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            return b && count.getCount() == 0 ? new Result("成功上传所有文件"):new Result("上传失败");
        } catch (InterruptedException e) {
            e.printStackTrace();
            return new Result(e);
        }
    }



    //String host, int port, String username, String password, String remotePath, String fileName, String localPath
    // 不能直接复制windows路径
    //System.out.println("\u202AC:/Users/Amo/Desktop/ftpTest");
    //System.out.println("C:/Users/Amo/Desktop/ftpTest");
    //[
    //  {
    //    "fileName": "FTP.txt",
    //    "localPath": "C:/Users/Amo/Desktop/ftpTest",
    //    "remotePath": "/home/zhangyuming",
    //    "url": "medusa002"
    //  }
    //]
    @ApiOperation("通过url和路径向每个集群发送download请求，下载集群的某各文件到本地文件夹")
    @PostMapping("/downByUrlAndPath")
    @ApiImplicitParam(name = "clusterFileDownloadInfoList",paramType = "body",value = "集群url和集群路径",dataType ="ClusterFileDownloadInfo",allowMultiple = true)
    public Result downByUrlAndPath(@RequestBody List<ClusterFileDownloadInfo> clusterFileDownloadInfoList) {
        CountDownLatch count = new CountDownLatch(clusterFileDownloadInfoList.size());
        ExecutorService taskExecutor = Executors.newFixedThreadPool(clusterFileDownloadInfoList.size());
        for (ClusterFileDownloadInfo clusterFileDownloadInfo : clusterFileDownloadInfoList) {

            taskExecutor.execute(() -> {
                try {
                    ftpService.downloadFile(clusterFileDownloadInfo.getUrl(), 21, username, password, clusterFileDownloadInfo.getRemotePath()
                            , clusterFileDownloadInfo.getFileName(), clusterFileDownloadInfo.getLocalPath());
                    count.countDown();
                }catch (Exception e){
                    e.printStackTrace();
                }
            });
        }
        taskExecutor.shutdown();
        try {
            boolean b = taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            return b && count.getCount() == 0 ? new Result("成功下载所有文件"):new Result("下载失败");
        } catch (InterruptedException e) {
            e.printStackTrace();
            return new Result(e);
        }
    }
}
