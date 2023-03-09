package com.szubd.mcdfs_and_rspc.service;

public interface DistCpService {
    int distcp(String srcHdfsPath, String destHdfsPath);
}
