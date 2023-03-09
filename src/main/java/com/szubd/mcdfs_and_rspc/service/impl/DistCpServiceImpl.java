package com.szubd.mcdfs_and_rspc.service.impl;

import com.szubd.mcdfs_and_rspc.service.DistCpService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Service
public class DistCpServiceImpl implements DistCpService {
    private Configuration configuration;

    @PostConstruct
    public void getDefaultConf() throws IOException {
        Configuration conf = new Configuration();
        System.setProperty("HADOOP_USER_NAME","hdfs");
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");

        FileSystem hdfs = FileSystem.get(conf);
        this.configuration = conf;
    }

    @Override
    public int distcp(String srcHdfsPath, String destHdfsPath) {
        try {
            Path srcPath = new Path(srcHdfsPath);
            Path destPath = new Path(destHdfsPath);
            DistCpOptions.Builder options = new DistCpOptions.Builder(srcPath, destPath);
//            //不需要同步
//            options.setSyncFolder(false);
//            options.setDeleteMissing(false);
//            //同副本复制需要校验filechecksums
//            options.setSkipCRC(false);
//            //不能忽略map错误
//            options.setIgnoreFailures(false);
//            //核心数据需要覆盖目标目录已经存在的文件
//            options.setOverwrite(true);
//            options.setBlocking(true);
//            options.maxMaps(100)
//            options.setMapBandwidth(10);
            options.preserve(DistCpOptions.FileAttribute.USER);
            options.preserve(DistCpOptions.FileAttribute.GROUP);
            options.preserve(DistCpOptions.FileAttribute.TIMES);
            options.preserve(DistCpOptions.FileAttribute.PERMISSION);
            options.preserve(DistCpOptions.FileAttribute.REPLICATION);
            //必须确保块大小相同，否则会导致filechecksum不一致
            options.preserve(DistCpOptions.FileAttribute.BLOCKSIZE);
            options.preserve(DistCpOptions.FileAttribute.CHECKSUMTYPE);

            final String[] argv = {"-update", srcHdfsPath, destHdfsPath};
            DistCp distCp = new DistCp(configuration, options.build());
            return ToolRunner.run(configuration, distCp, argv);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }
}
