package com.szubd.mcdfs_and_rspc.bean;

import lombok.Data;

@Data
public class ClusterFileUploadInfo {
    private String url;
    private String basePath;
    private String filePath;//最后一个文件夹
    private String localFileName;
    private String localPath;



}
