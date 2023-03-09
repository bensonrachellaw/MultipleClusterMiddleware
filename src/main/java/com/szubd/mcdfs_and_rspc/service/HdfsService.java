package com.szubd.mcdfs_and_rspc.service;

import com.szubd.mcdfs_and_rspc.bean.FileInfo;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface HdfsService{
    Map<String, FileSystem> init(List<String> urls, String user);
    List<FileInfo> listFiles(String path, FileSystem fileSystem) throws IOException;
    void uploadFile(MultipartFile file, String path, FileSystem fileSystem) throws IOException;
    void uploadFileByLocalPath(String localPath, String remotePath, FileSystem fileSystem) throws IOException;
    boolean delete(String path, FileSystem fileSystem) throws IOException;
}
