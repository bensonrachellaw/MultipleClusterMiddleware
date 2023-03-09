package com.szubd.mcdfs_and_rspc.service;

import com.szubd.mcdfs_and_rspc.bean.SparkJob;
import com.szubd.mcdfs_and_rspc.eum.SparkJobState;

import java.util.List;
import java.util.Map;

public interface RSPcService {
    /**
     * 运行一个 SparkJob 一直等到他运行完成之后才会有返回值
     * 期间伴随着日志的输出
     *
     * @param job SparkJob
     * @return 任务是否正确运行结束
     */
    void initUrl(String url);
    boolean runSparkJob(SparkJob job);

    /**
     * 后台启动一个 SparkJob
     *
     * @param job SparkJob
     * @return SparkJob 的 batch session ID
     */
    int runSparkJobBackground(SparkJob job);

    /**
     * 启动一个 session 运行 SparkJob, 不需要等待是否运行成功
     *
     * @param job SparkJob
     * @return SparkJob 的 batch session ID
     */
    int startSparkJob(SparkJob job);

    /**
     * 查询所有的 活跃的 Spark Job
     *
     * @return 所有活跃的 Spark Job = batch session
     */
    Map<String, Object> getActiveSparkJobs();

    /**
     * 查询具体的且活跃的 Spark Job 信息
     *
     * @param sparkJobID SparkJob 的 ID（batch session ID）
     * @return Spark Job 信息 ，具体的 batch session 信息
     */
    Map<String, Object> getSparkJobInfo(int sparkJobID);

    /**
     * 查询具体的且活跃的 Spark Job 状态
     *
     * @param sparkJobID SparkJob 的 ID（batch session ID）
     * @return Spark Job 状态 ，具体的 batch session 状态
     */
    SparkJobState getSparkJobState(int sparkJobID);

    /**
     * 查询具体的且活跃的 Spark Job 日志
     *
     * @param sparkJobID SparkJob 的 ID（batch session ID）
     * @return Spark Job 日志 ，具体的 batch session 日志
     */
    String getSparkJobLog(int sparkJobID);

    /**
     * 返回新增加的日志
     * 在前端实时展示日志的时候有用
     * 每隔几秒请求一下这个接口请求这个
     *
     * @param sparkJobID SparkJob 的 ID（batch session ID）
     * @param oldLog     之前返回的日志
     * @return 新生成的日志
     */
    Map<String, List<String>> getSparkJobNewLog(int sparkJobID, Map<String, List<String>> oldLog);

    /**
     * Kills the Batch job.
     *
     * @param sparkJobID SparkJob 的 ID（batch session ID）
     * @return msg
     * {
     * "msg": "deleted"
     * }
     */
    Map<String, Object> deleteSparkJob(int sparkJobID);

}
