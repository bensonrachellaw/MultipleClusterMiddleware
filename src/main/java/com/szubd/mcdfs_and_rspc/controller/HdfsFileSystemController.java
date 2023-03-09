package com.szubd.mcdfs_and_rspc.controller;

import com.szubd.mcdfs_and_rspc.bean.FileInfo;
import com.szubd.mcdfs_and_rspc.bean.Result;
import com.szubd.mcdfs_and_rspc.service.HdfsService;
import com.szubd.mcdfs_and_rspc.service.YarnService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.*;

@RequestMapping("/hdfs")
@RestController
@Api("hdfs文件管理")
public class HdfsFileSystemController {
    // TODO: url和user不再使用外部注入
    //
    @Value("#{'${hadoop.url}'.split(',')}")
    private List<String> urls;
    @Value("${hadoop.user}")
    private String user;

    @Autowired
    private HdfsService hdfsService;

    private Logger logger = LogManager.getLogger(this.getClass());
    private Map<String, FileSystem> fileSystemMap = null;

    // 在构造函数执行之后才会执行该注解标注的方法
    @PostConstruct
    public void init() {
        this.fileSystemMap = hdfsService.init(urls, user);
    }

    @ApiOperation("列举hdfs文件目录")
    @PostMapping("/ls")
    @ApiImplicitParam(name = "pathWithUrl",paramType = "body",value = "hdfs位置和hdfs路径")
    public Result ls(@RequestBody Map<String, String> pathWithUrl) {
        Set<String> urls = fileSystemMap.keySet();
        Map<String,List<FileInfo>> resultMap = new HashMap<>();
        for (String url : pathWithUrl.keySet()) {
            if(urls.contains(url)) {
                FileSystem fileSystem = fileSystemMap.get(url);
                try {
                    List<FileInfo> fileInfoList = hdfsService.listFiles(pathWithUrl.get(url), fileSystem);
                    resultMap.put(url, fileInfoList);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else {
                resultMap.put(url, null);
            }
        }
        return new Result(resultMap);
    }


//    @ApiOperation("上传文件")
//    @PostMapping("/put")
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = "file",value = "文件",dataType = "MultipartFile"),
//            @ApiImplicitParam(name = "path",value = "hdfs路径",dataType = "String")
//    })
//    public Result put(MultipartFile file, String path) {
//        if (file.isEmpty()) {
//            return new Result(-1, "上传文件异常");
//        }
//        try {
//            boolean fileExist = fileSystem.exists(new Path(path));
//            if (fileExist) {
//                return new Result(-1, "远端文件已存在");
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        try {
//            hdfsService.uploadFile(file, path, fileSystem);
//        } catch (IllegalArgumentException | IOException e) {
//            logger.error("上传文件出错", e);
//            return new Result(-1, "上传失败");
//        }
//        return new Result();
//    }
//
//
//    @ApiOperation("通过路径上传文件")
//    @GetMapping("/putWithPath")
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = "localPath",value = "本地文件路径",dataType = "String"),
//            @ApiImplicitParam(name = "remotePath",value = "hdfs路径",dataType = "String")
//    })
//    public Result putWithPath(String localPath, String remotePath) {
//        File file = new File(localPath);
//        if(!file.exists()){
//            return new Result(-1, "本地文件不存在");
//        }
//        try {
//            boolean fileExist = fileSystem.exists(new Path(remotePath));
//            if (fileExist) {
//                return new Result(-1, "远端文件已存在");
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        try {
//            hdfsService.uploadFileByLocalPath(localPath, remotePath, fileSystem);
//        } catch (IllegalArgumentException | IOException e) {
//            logger.error("上传文件出错", e);
//            return new Result(-1, "上传失败");
//        }
//        return new Result();
//    }
//
//    //TODO: 这里的response可以改成指定本地路径而非浏览器下载
//    @ApiOperation("下载文件")
//    @GetMapping("/download")
//    @ApiImplicitParam(name = "path",value = "hdfs路径",dataType = "String")
//    public Result download(HttpServletResponse response, String path) {
//        InputStream input = null;
//        try {
//            input = fileSystem.open(new Path(path));
//            IOUtils.copyBytes(input, response.getOutputStream(), 4096, true);
//            return new Result();
//        } catch (IllegalArgumentException | IOException e) {
//            e.printStackTrace();
//            return new Result(-1, "下载失败");
//        }
//    }
//
//    @ApiOperation("删除hdfs文件")
//    @GetMapping("/delete")
//    @ApiImplicitParam(name = "path",value = "hdfs路径",dataType = "String")
//    public Result delete(String path) {
//        try {
//            boolean result = hdfsService.delete(path, fileSystem);
//            System.out.println(path + " " + result);
//            if (result) {
//                return new Result();
//            }
//            return new Result(-1, "删除失败");
//        } catch (IllegalArgumentException | IOException e) {
//            logger.error("删除文件出错", e);
//        }
//        return new Result();
//    }

}
