package com.khalifa;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.khalifa.protocol.QueryProtocol.Query;


public class APIClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = Logger.getLogger( APIClientHandler.class.getName());

    private final BlockingQueue<Object> answer = new LinkedBlockingQueue<Object>();
    public Object getData(Channel channel,Query.Builder query) {
    	channel.writeAndFlush(query.build());
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
    public Object getDataDirect(Channel channel,Query.Builder query) {
    	if(channel.isActive())
    		channel.writeAndFlush(query.build());
        Object localTimes=null;
		try {
			localTimes = answer.poll(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        return localTimes;
    }
    public void channelRead(ChannelHandlerContext ctx,Object msg) {
    	System.out.println("channelRead");
    	if(msg!=null){
    		ReferenceCountUtil.release(msg);
    		answer.offer(msg);
    	}
    }
	public void channelInactive(ChannelHandlerContext ctx)      throws Exception{
		System.out.println("channel inactive");
		ctx.close();
	}

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable e) {
    	e.printStackTrace();
        logger.log( Level.WARNING, "Unexpected exception from downstream.",e.getCause());
        try{
        	answer.add(e.getCause().getMessage());
		if(ctx.channel().isActive()){
			ctx.close();
		}
        }catch(Exception ee){}
    }
}