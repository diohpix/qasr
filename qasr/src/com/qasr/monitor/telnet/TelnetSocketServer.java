package com.qasr.monitor.telnet;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * A HTTP server which serves Web Socket requests at:
 *
 * http://localhost:8080/websocket
 *
 * Open your browser at http://localhost:8080/, then the demo page will be loaded and a Web Socket connection will be
 * made automatically.
 *
 * This server illustrates support for the different web socket specification versions and will work with:
 *
 * <ul>
 * <li>Safari 5+ (draft-ietf-hybi-thewebsocketprotocol-00)
 * <li>Chrome 6-13 (draft-ietf-hybi-thewebsocketprotocol-00)
 * <li>Chrome 14+ (draft-ietf-hybi-thewebsocketprotocol-10)
 * <li>Chrome 16+ (RFC 6455 aka draft-ietf-hybi-thewebsocketprotocol-17)
 * <li>Firefox 7+ (draft-ietf-hybi-thewebsocketprotocol-10)
 * <li>Firefox 11+ (RFC 6455 aka draft-ietf-hybi-thewebsocketprotocol-17)
 * </ul>
 */
public class TelnetSocketServer implements Runnable {

    private  int port;
    public TelnetSocketServer(int port) {
        this.port = port;
    }

    public void run() {
        // Configure the server.
    	ExecutorService e1=  Executors.newCachedThreadPool();
    	ExecutorService e2=  Executors.newCachedThreadPool();
        ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory( e1,e2));

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new TelnetServerPipelineFactory());

        // Bind and start to accept incoming connections.
        try{
        	bootstrap.bind(new InetSocketAddress(port));
        	System.out.println("Web socket server started at port " + port + '.');
        	System.out.println("Open your browser and navigate to http://localhost:" + port + '/');
        }catch(Exception e){
        	System.out.println(e.getCause().getMessage());
        	e1.shutdown();
        	e2.shutdown();
        	System.exit(-1);
        }
    }

}
