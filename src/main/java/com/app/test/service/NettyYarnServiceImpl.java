package com.app.test.service;

import com.app.test.service.NettyYarnService;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;


// 放在netty 服务端
// 同时路径必须统一一致
@Service
public class NettyYarnServiceImpl implements NettyYarnService {

    @Override
    public String getAppLog(String appIdStr) {
        Configuration conf = new YarnConfiguration();
        conf.addResource(new Path("/etc/hadoop/conf.cloudera.yarn/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf.cloudera.yarn/yarn-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf.cloudera.yarn/hdfs-site.xml"));
        if(appIdStr == null || appIdStr.equals(""))
        {
            System.out.println("appId is null!");
            return null;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);

        ApplicationId appId = null;
        appId = ConverterUtils.toApplicationId(appIdStr);

        Path remoteRootLogDir = new Path(conf.get("yarn.nodemanager.remote-app-log-dir", "/tmp/logs"));
        System.out.println(remoteRootLogDir);
        String user =  "longhao"; // UserGroupInformation.getCurrentUser().getShortUserName();
        String logDirSuffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(conf);

        // 这个app的user 当前起jar的user 和 user要一致
        Path remoteAppLogDir = LogAggregationUtils.getRemoteAppLogDir(remoteRootLogDir, appId, user, logDirSuffix);
        RemoteIterator<FileStatus> nodeFiles;
        System.out.println(remoteAppLogDir);
        try
        {
            Path qualifiedLogDir = FileContext.getFileContext(conf).makeQualified(remoteAppLogDir);
            nodeFiles = FileContext.getFileContext(qualifiedLogDir.toUri(), conf).listStatus(remoteAppLogDir);
        }
        catch (Exception e)
        {
            System.out.println("文件找不到");
            return null;
        }

        boolean foundAnyLogs = false;
        try {
            while (nodeFiles.hasNext())
            {
                FileStatus thisNodeFile = (FileStatus)nodeFiles.next();
                if (!thisNodeFile.getPath().getName().endsWith(".tmp"))
                {
                    System.out.println("NodeFileName = "+thisNodeFile.getPath().getName());
                    AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(conf, thisNodeFile.getPath());
                    try
                    {
                        AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
                        DataInputStream valueStream = reader.next(key);
                        for (;;)
                        {
                            if (valueStream != null)
                            {
                                String containerString = "\n\nContainer: " + key + " on " + thisNodeFile.getPath().getName();

                                out.println(containerString);
                                out.println(StringUtils.repeat("=", containerString.length()));
                                try
                                {
                                    for (;;)
                                    {
                                        AggregatedLogFormat.LogReader.readAContainerLogsForALogType(valueStream, out, thisNodeFile.getModificationTime());

                                        foundAnyLogs = true;
                                    }

                                }
                                catch (EOFException eof)
                                {
                                    key = new AggregatedLogFormat.LogKey();
                                    valueStream = reader.next(key);

                                }

                            }else{
                                break;
                            }
                        }
                    }
                    finally
                    {
                        reader.close();
                    }
                }
            }
        }catch (Exception e){
            System.out.println("文件错误");
            return null;
        }
        if (!foundAnyLogs)
        {
            System.out.println("没有任何log");
            return null;
        }
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }
}
