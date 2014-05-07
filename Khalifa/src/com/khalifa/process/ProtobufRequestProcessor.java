package com.khalifa.process;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.khalifa.db.APIService;
import com.khalifa.db.proxy.ProxySqlSession;
import com.khalifa.protocol.QueryProtocol.Query;
import com.khalifa.protocol.QueryProtocol.Response;
import com.khalifa.util.CommonData;
import com.khalifa.util.ResponseUtil;
import com.khalifa.util.UK;

public class ProtobufRequestProcessor  {
	
	private static final Logger logger = LoggerFactory.getLogger(ProtobufRequestProcessor.class);
    
	private static int expire = CommonData.redis_default_expire;
	private ChannelHandlerContext ctx;
	private Query q;
	
	public ProtobufRequestProcessor(ChannelHandlerContext ctx,Query e) {
		this.q = e;
		this.ctx = ctx;
	}
	
	public void run() throws Exception {
		State state = ctx.channel().attr(CommonData.STATE).get();
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
				ctx.channel().attr(CommonData.STATE).set(state);
			}else{
				state.addLog("|");
				state.addLog(SQL);
			}
			if(type==5){ // transaction command
				if("BEGIN_TRANSACTION".equals(SQL)){
					SqlSession sess = APIService.getBatchSession(q.getDbname());
					sess.getConnection().setAutoCommit(false);
					sess.clearCache();
					state.setSession(sess);
					ResponseUtil.makeResponse(ctx, 200, "TRANSACTION_START");
					return;
				}else if("BEGIN_R_TRANSACTION".equals(SQL)){
					SqlSession sess = APIService.getSession(q.getDbname());
					sess.clearCache();
					state.setSession(sess);
					state.setReadTransaction(true);
					ResponseUtil.makeResponse(ctx, 200, "TRANSACTION_START");
					return;
				}else if("END_TRANSACTION".equals(SQL)){
					SqlSession sess =state.getSession();
					if(sess!=null){
						sess.getConnection().close();
						state.setSession(null);
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
				}else if("OUTPUTPARAM".equals(SQL)){
					logger.debug("OUTPUTPARAM");
					Response.Builder res = state.getOutputParam();
					if(res!=null){
						res.setCode(200);
						ChannelFuture f = ctx.writeAndFlush(res.build());
					}else{
						ResponseUtil.makeResponse(ctx, 201, "NO_OUTPUTPARAM");
					}
					return;
				}
			}
			List<Query.Data> queryList = q.getDataList();
			Object list = null;
			if(queryList.size()==1){
				Map<String,Object> _where = UK.getWhere(queryList.get(0));
				int _exp = q.hasExpire() ? q.getExpire() : expire;
				if(state.getSession()!=null && !state.isReadTransaction()) { // transaction
					list = APIService.transactionQuery((ProxySqlSession)state.getSession(), SQL, _where,state);
				}else if(state.getSession()!=null && state.isReadTransaction()) { // read only transaction
					list = APIService.readOnlyQuery((ProxySqlSession)state.getSession(),q.getDbname(),type,SQL, _where,UK.getWhereString(_where),_exp,state);
				}else{ // normal SQL
					list = APIService.query(q.getDbname(),type,SQL, _where,UK.getWhereString(_where),_exp,state);
				}
			}else{ // batch process
				boolean tx=false;
				List<Map<String,Object>> mlist = new ArrayList<Map<String,Object>>(queryList.size());
				for (Query.Data data : queryList) {
					Map<String,Object> _where = UK.getWhere(data);
					mlist.add(_where);
				}
				ProxySqlSession sess = null;
				if(state.getSession()==null){
					sess = (ProxySqlSession) APIService.getBatchSession(q.getDbname());
					sess.getConnection().setAutoCommit(false);
				}else{
					sess = (ProxySqlSession) state.getSession();
					sess.getConnection().setAutoCommit(false);
					tx=true;
				}
				list = APIService.batchQuery(sess, SQL,mlist, state,tx);
			}
			Response r = null;
			if(list instanceof byte[]){
				r = Response.PARSER.parseFrom((byte[])list);
			}else{
				Response.Builder res = null;
				res = (Response.Builder) list;
				res.setCode(200);
				r = res.build();
				res.clear();
			}
	        ChannelFuture f = ctx.writeAndFlush(r);
	       /* if(state.getSession()==null){
	        	f.addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
					//	((SocketChannel)future.channel()).shutdownOutput(); // half_close socket
					}
				});	
	        }*/
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
			//event.getChannel().close();	//TODO 이걸 끊어야해 말아야해
				
		} finally {
			//if(event.getChannel().getAttachment()==null){
				//event.getChannel().close();
			//}
		}
	}

}