package com.netty.rpc.client.handler;

import com.netty.rpc.client.connect.ConnectionManager;
import com.netty.rpc.common.codec.RpcRequest;
import com.netty.rpc.common.codec.RpcResponse;
import com.netty.rpc.common.codec.RpcResponseId;
import com.netty.rpc.common.protocol.RpcProtocol;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.netty.rpc.common.codec.RpcResponseId.*;

/**
 * Created by luxiaoxun on 2016-03-14.
 */
// 通过唯一标识 rpcProtocol 标识哪台服务器
public class RpcClientHandler extends SimpleChannelInboundHandler<RpcResponse> {
    private static final Logger logger = LoggerFactory.getLogger(RpcClientHandler.class);

    // TODO:可能要使用redis进行缓存
    private ConcurrentHashMap<String, RpcFuture> pendingRPC = new ConcurrentHashMap<>();
    public static final Map<String, Promise<Object>> PROMISES = new ConcurrentHashMap<>();
    // handler与channel相绑定
    private volatile Channel channel;
    private SocketAddress remotePeer;
    private RpcProtocol rpcProtocol;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.remotePeer = this.channel.remoteAddress();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        this.channel = ctx.channel();
    }

    @Override
    // 接收服务器的结果
    public void channelRead0(ChannelHandlerContext ctx, RpcResponse response) throws Exception {
        // 接受服务端心跳，即集群的状态
        if(response.getRequestId().equals(SPARK_HEART_BEAT_ID)){
            System.out.println(String.valueOf(response.getResult()));
            //不让他继续走
            return;
        }
        // 接受服务器的提示消息
        if(response.getRequestId().equals(WAIT_MISSION_ID)){
            System.out.println(String.valueOf(response.getResult()));
            return;
        }
        //接收服务器的block传输结果
        if(response.getRequestId().equals(TRANSFER_ID)){
            System.out.println(String.valueOf(response.getResult()));
            return;
        }
        String requestId = response.getRequestId();
        logger.debug("Receive response: " + requestId);
        // 通知相应的future进行唤醒
        RpcFuture rpcFuture = pendingRPC.get(requestId);
        if (rpcFuture != null) {
            pendingRPC.remove(requestId);
            // 向future中填充结果 并唤醒阻塞
            rpcFuture.done(response);
        } else {
            System.out.println("隔离其他非此次发送的任务id");
            logger.warn("Can not get pending response for request id: " + requestId);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Client caught exception: " + cause.getMessage());
        ctx.close();
    }

    public void close() {
        channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE); // 写空数据并关闭
    }

    public RpcFuture sendRequest(RpcRequest request) {
        RpcFuture rpcFuture = new RpcFuture(request);
        pendingRPC.put(request.getRequestId(), rpcFuture);
        try {
            ChannelFuture channelFuture = channel.writeAndFlush(request).sync(); // 发送消息 看看是否发送成功
            if (!channelFuture.isSuccess()) {
                logger.error("Send request {} error", request.getRequestId());
            }
        } catch (InterruptedException e) {
            logger.error("Send request exception: " + e.getMessage());
        }

        return rpcFuture;
    }

    @Override
    // 发送心跳包
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        IdleStateEvent event = (IdleStateEvent) evt;
        if (event.state() == IdleState.WRITER_IDLE) {
            //Send ping
            sendRequest(RpcResponseId.BEAT_PING);
            logger.debug("Client send beat-ping to " + remotePeer);
        }
        else {
            super.userEventTriggered(ctx, evt);
        }
    }

    public void setRpcProtocol(RpcProtocol rpcProtocol) {
        this.rpcProtocol = rpcProtocol;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        ConnectionManager.getInstance().removeHandler(rpcProtocol);
    }
}
