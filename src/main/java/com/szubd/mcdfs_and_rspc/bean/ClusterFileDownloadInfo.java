package com.szubd.mcdfs_and_rspc.bean;

import lombok.Data;

@Data
public class ClusterFileDownloadInfo {
    private String url;
    private String remotePath;
    private String fileName;
    private String localPath;
}
