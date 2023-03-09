package com.szubd.mcdfs_and_rspc.webSocket;

import org.springframework.stereotype.Component;

import javax.websocket.OnClose;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Component
@ServerEndpoint("/websocket/{userId}")  // 接口路径 ws://localhost:8087/webSocket/userId;
public class RealTimeRSPcWebSocket {
    //与某个客户端的连接会话，需要通过它来给客户端发送数据
    private Session session;

    private String userId;

    //concurrent包的线程安全Set，用来存放每个客户端对应的MyWebSocket对象。
    //虽然@Component默认是单例模式的，但springboot还是会为每个websocket连接初始化一个bean，所以可以用一个静态set保存起来。
    //  注：底下WebSocket是当前类名
    public static CopyOnWriteArraySet<RealTimeRSPcWebSocket> webSockets =new CopyOnWriteArraySet<>();
    // 用来存在线连接用户信息
    private static ConcurrentHashMap<String,Session> sessionPool = new ConcurrentHashMap<String,Session>();


    @OnOpen
    public void onOpen(Session session, @PathParam(value="userId")String userId) {
        try {
            this.session = session;
            this.userId = userId;
            webSockets.add(this);
            sessionPool.put(userId, session);
        } catch (Exception e) {
        }
    }

    // 链接关闭调用的方法
    @OnClose
    public void onClose() {
        try {
            webSockets.remove(this);
            sessionPool.remove(this.userId);
        } catch (Exception e) {
        }
    }

    // 此为广播消息
    public void sendAllMessage(String message) {
        for(RealTimeRSPcWebSocket webSocket : webSockets) {
            try {
                if(webSocket.session.isOpen()) {
                    webSocket.session.getAsyncRemote().sendText(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
