package com.szubd.mcdfs_and_rspc.service.impl;

import com.szubd.mcdfs_and_rspc.bean.FileInfo;

import com.szubd.mcdfs_and_rspc.service.HdfsService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HdfsServiceImpl implements HdfsService {

    @Override
    public Map<String, FileSystem> init(List<String> urls, String user){
        Configuration configuration = new Configuration();
        Map<String, FileSystem> fileSystemMap = new HashMap<>();
        for (String url : urls) {
            FileSystem fileSystem = null;
            try {
                fileSystem = FileSystem.get(new URI(url), configuration, user);
            } catch (Exception e) {
                e.printStackTrace();
            }
            fileSystemMap.put(url, fileSystem);
        }
        return fileSystemMap;
    }

    @Override
    public List<FileInfo> listFiles(String path, FileSystem fileSystem) throws IOException {
        List<FileInfo> fileInfoList = new ArrayList<>();
        FileStatus[] statuses = fileSystem.listStatus(new Path(path));
        DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        for (FileStatus file : statuses) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setFileName(file.getPath().getName());
            fileInfo.setDirectory(file.isDirectory());
            fileInfo.setSize(file.getLen());
            fileInfo.setUpdateTime(ftf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(file.getModificationTime()), ZoneId.systemDefault())));
            fileInfoList.add(fileInfo);
        }
        return fileInfoList;
    }

    @Override
    public void uploadFile(MultipartFile file, String path, FileSystem fileSystem) throws IOException {
        InputStream input = null;
        OutputStream output = null;

        input = file.getInputStream();
        output = fileSystem.create(new Path(path));
        IOUtils.copyBytes(input, output, 4096, true);
    }

    @Override
    public void uploadFileByLocalPath(String localPath, String remotePath, FileSystem fileSystem) throws IOException {
        // 待上传的文件路径(windows)
        Path src = new Path(localPath);
        // 上传之后存放的路径(HDFS)
        Path dst = new Path(remotePath);
        // 上传
        fileSystem.copyFromLocalFile(src,dst);
    }

    @Override
    public boolean delete(String path, FileSystem fileSystem) throws IOException {
        return fileSystem.delete(new Path(path), true);
    }
}
