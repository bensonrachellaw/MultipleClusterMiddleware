package com.szubd.mcdfs_and_rspc.bean;

import lombok.Data;

@Data
public class FileInfo {
    private String fileName;
    private long size;
    private String updateTime;
    private boolean directory;
}
