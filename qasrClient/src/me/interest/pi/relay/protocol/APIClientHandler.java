package me.interest.pi.relay.protocol;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import me.interest.pi.relay.protocol.QueryProtocol.Query;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

public class APIClientHandler extends SimpleChannelHandler {

    private static final Logger logger = Logger.getLogger( APIClientHandler.class.getName());

    private volatile Channel channel;
    private final BlockingQueue<Object> answer = new LinkedBlockingQueue<Object>();
    public Object getData(Query.Builder query) {
    	channel.write(query.build());
        Object localTimes;
        boolean interrupted = false;
        for (;;) {
            try {
                localTimes = answer.take();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return localTimes;
    }
    public Object getDataDirect(Query.Builder query) {
    	if(channel.isConnected())
    		channel.write(query.build());
        Object localTimes=null;
		try {
			localTimes = answer.poll(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return localTimes;
    }
    @Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        	super.handleDownstream(ctx, e);
    }
    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
    		super.handleUpstream(ctx, e);
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channel = e.getChannel();
        super.channelOpen(ctx, e);
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, final MessageEvent e) {
    	if(e.getMessage()!=null){
    		answer.offer((Object) e.getMessage());
    	}
    }
    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception{
    	super.channelDisconnected(ctx, e);
    }
    
    @Override
    public void closeRequested(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception{
    	super.closeRequested(ctx, e);
    }
    
    @Override
    public void disconnectRequested(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception{
    	super.disconnectRequested(ctx, e);
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.log( Level.WARNING, "Unexpected exception from downstream.",e.getCause());
        try{
        	answer.add(e.getCause().getMessage());
		if(e.getChannel().isConnected()){
			e.getChannel().close();
		}
        }catch(Exception ee){}
    }
}
