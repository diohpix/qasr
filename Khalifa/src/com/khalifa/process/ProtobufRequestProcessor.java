package com.khalifa.process;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;

import java.util.Map;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.khalifa.db.APIService;
import com.khalifa.db.proxy.ProxySqlSession;
import com.khalifa.protocol.QueryProtocol.Query;
import com.khalifa.protocol.QueryProtocol.Response;
import com.khalifa.protocol.server.protobuf.CommonData;
import com.khalifa.util.Configure;
import com.khalifa.util.ResponseUtil;
import com.khalifa.util.UK;

public class ProtobufRequestProcessor  {
	
	private static final Logger logger = LoggerFactory.getLogger(ProtobufRequestProcessor.class);
    
	private static int expire = Configure.getIntProperty("redis.default_expire");
	private ChannelHandlerContext ctx;
	private Query q;
	
	public ProtobufRequestProcessor(ChannelHandlerContext ctx,Query e) {
		this.q = e;
		this.ctx = ctx;
	}
	
	public void run() throws Exception {
		State state = ctx.attr(CommonData.STATE).get();
		try {
			int type = q.getQueryType();
			String SQL = q.getCommand().toStringUtf8();
			if(SQL == null){
				ctx.close();
				return;
			}
			if(state==null){
				state = new State();
				state.addLog(SQL);
				ctx.attr(CommonData.STATE).set(state);
			}else{
				state.addLog("|");
				state.addLog(SQL);
			}
			Map<String,Object> _where = UK.getWhere(q);
			if("BEGIN_TRANSACTION".equals(SQL)){
				SqlSession sess = APIService.getSession(q.getDbname());
				sess.getConnection().setAutoCommit(false);
				sess.clearCache();
				state.setSession(sess);
				ResponseUtil.makeResponse(ctx, 200, "TRANSACTION_START");
				return;
			}else if("END_TRANSACTION".equals(SQL)){
				SqlSession sess =state.getSession();
				if(sess!=null){
					state.setSession(null);
					sess.close();
					ResponseUtil.makeResponse(ctx, 200, "TRANSACTION_END");
				}else{
					ResponseUtil.makeResponse(ctx, 201, "NO_TRANSACTION");
				}
				return;
			}else if("ROLLBACK".equals(SQL)){
				SqlSession sess =state.getSession();
				if(sess!=null){
					sess.getConnection().rollback();
					ResponseUtil.makeResponse(ctx, 200, "ROLLBACK_SUCCESS");
				}else{
					ResponseUtil.makeResponse(ctx, 201, "NO_TRANSACTION");
				}
				return;
			}else if("COMMIT".equals(SQL)){
				SqlSession sess =state.getSession();
				if(sess!=null){
					sess.getConnection().commit();
					ResponseUtil.makeResponse(ctx, 200, "COMMIT_SUCCESS");
				}else{
					ResponseUtil.makeResponse(ctx, 201, "NO_TRANSACTION");
				}
				return;
			}
			Object list = null;
			if(state.getSession()!=null){
				list = APIService.transactionQuery((ProxySqlSession)state.getSession(), SQL, _where);
			}else{
				int _exp = q.hasExpire() ? q.getExpire() : expire;
				list = APIService.query(q.getDbname(),type,SQL, _where,UK.getWhereString(_where),_exp);
			}
			Response r = null;
			if(list instanceof byte[]){
				r = Response.PARSER.parseFrom((byte[])list);
			}else{
				Response.Builder res = null;
				res = (Response.Builder) list;
				res.setCode(200);
				r = res.build();
			}
	        ChannelFuture f = ctx.writeAndFlush(r);
	        if(state.getSession()==null){
	        	f.addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						((SocketChannel)future.channel()).shutdownOutput(); // half_close socket
					}
				});	
	        }
		} catch (Exception e) {
			throw e;
			//event.getChannel().close();	//TODO 이걸 끊어야해 말아야해
				
		} finally {
			//if(event.getChannel().getAttachment()==null){
				//event.getChannel().close();
			//}
		}
	}

}