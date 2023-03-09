package com.netty.rpc.common.codec;

public final class RpcResponseId {

    public static final int BEAT_INTERVAL = 30;
    public static final int BEAT_TIMEOUT = 3 * BEAT_INTERVAL;
    public static final String BEAT_ID = "BEAT_PING_PONG";
    public static final String SPARK_HEART_BEAT_ID = "SPARK_BEAT_PING_PONG";
    public static final String WAIT_MISSION_ID = "WAIT";
    public static final String TRANSFER_ID = "TRANS";
    public static final String BLOCK_ID = "BLOCK";

    public static RpcRequest BEAT_PING;

    static {
        BEAT_PING = new RpcRequest() {};
        BEAT_PING.setRequestId(BEAT_ID);
    }

}
