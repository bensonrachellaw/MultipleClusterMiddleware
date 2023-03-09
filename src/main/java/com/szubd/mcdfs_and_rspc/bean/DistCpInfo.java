package com.szubd.mcdfs_and_rspc.bean;

import lombok.Data;

@Data
public class DistCpInfo {
    private String srcHdfsPath;
    private String destHdfsPath;
}
