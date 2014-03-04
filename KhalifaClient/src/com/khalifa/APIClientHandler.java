package com.khalifa;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.khalifa.protocol.QueryProtocol.Query;
import com.khalifa.protocol.QueryProtocol.Response;


public class APIClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(APIClientHandler.class);

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
        Object response=null;
		try {
			response = answer.poll(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if(logger.isDebugEnabled()){
			Response res = (Response) response;
			logger.debug("GET Response {}",res);
		}
        return response;
    }
    public void channelRead(ChannelHandlerContext ctx,Object msg) {
    	if(msg!=null){
    		ReferenceCountUtil.release(msg);
    		answer.offer(msg);
    	}
    }
	public void channelInactive(ChannelHandlerContext ctx)      throws Exception{
		logger.debug("channel inactive");
		ctx.close();
	}

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable e) {
    	e.printStackTrace();
        logger.warn( "Unexpected exception from downstream. {}",e.getCause());
        try{
        	answer.add(e.getCause().getMessage());
		if(ctx.channel().isActive()){
			ctx.close();
		}
        }catch(Exception ee){}
    }
}