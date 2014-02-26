package com.khalifa.protocol.server.http;

import static io.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.COOKIE;
import static io.netty.handler.codec.http.HttpHeaders.Names.SET_COOKIE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.Cookie;
import io.netty.handler.codec.http.CookieDecoder;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.ServerCookieEncoder;
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.khalifa.util.ResponseUtil;

public class HttpSnoopServerHandler extends SimpleChannelInboundHandler<Object> {

    private HttpRequest request;
    private String body=null;
    /** Buffer that stores the response content */

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest) {
            HttpRequest request = this.request = (HttpRequest) msg;
            if (is100ContinueExpected(request)) {
                send100Continue(ctx);
            }
        }
        if (msg instanceof HttpContent) {
            HttpContent httpContent = (HttpContent) msg;
            ByteBuf content = httpContent.content();
            if (content.isReadable()) {
            	if(body ==null){
            		body = content.toString(CharsetUtil.UTF_8);
            	}else{
            		body+=content.toString(CharsetUtil.UTF_8);
            	}
            }
            if (msg instanceof LastHttpContent) {
                LastHttpContent trailer = (LastHttpContent) msg;
                String buf = service();
                writeResponse(trailer, ctx,buf);
            }
        }
    }
    private String service(){
    	QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
        Map<String, List<String>> params = queryStringDecoder.parameters();
        String url = request.getUri();
        System.out.println(url);
        HttpMethod method = request.getMethod();
        String buf = null;
        if(body==null){
        	Map<String,Object> d = new HashMap<String, Object>(); 
        	for (Map.Entry<String, List<String>> u : params.entrySet()) {
        		d.put(u.getKey(), u.getValue().get(0));
			}
        	body = ResponseUtil.convertMap2JSONString(d);
        }
        if(url.startsWith("/dbproxy/")){
        	
        }else if(url.startsWith("/monitor/")){
        	buf = Monitor.service(method,url,params,body);
        }else{
        	
        }
        return buf;
    }
    

    private boolean writeResponse(HttpObject currentObj, ChannelHandlerContext ctx,String buf) {
        // Decide whether to close the connection or not.
        boolean keepAlive = isKeepAlive(request);
        // Build the response object.
        FullHttpResponse response = null;
        if(request.getUri().indexOf(".json")>-1){
        	response = new DefaultFullHttpResponse(HTTP_1_1, currentObj.getDecoderResult().isSuccess()? OK : BAD_REQUEST,  Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
        	response.headers().set(CONTENT_TYPE, "application/json;charset=UTF-8");
        }else{
        	StringBuilder b = new StringBuilder();
        	if(buf==null){
        		b.append("Not Found");
        		ctx.close();
        		return false;
        	}else{
	        	Map<String,Object> map = ResponseUtil.convertJSONString2Map(buf);
	        	List<Map<String,Object>> list = (List<Map<String, Object>>) map.get("list");
	        	
	        	int i=0;
	        	b.append("<table border='1'>");
	        	for (Map<String, Object> map2 : list) {
	        		
	        		if(i==0){
	        			b.append("<tr>");
	        			for (Map.Entry<String, Object> item : map2.entrySet()){
							b.append("<td>").append(item.getKey()).append("</td>");
	        			}	
	        			i++;
	        			b.append("</tr>");
	        		}
	        		b.append("<tr>");
	        		for (Map.Entry<String, Object> item : map2.entrySet()){
	        			if(item.getValue()instanceof Double){
	        				b.append("<td>").append( ((Double)item.getValue()).intValue()).append("</td>");
	        			}else{
	        				b.append("<td>").append(item.getValue()).append("</td>");
	        			}
	        		}
	        		b.append("</tr>");
				}
	        	b.append("</table>");
	        	response = new DefaultFullHttpResponse(HTTP_1_1, currentObj.getDecoderResult().isSuccess()? OK : BAD_REQUEST,  Unpooled.copiedBuffer(b.toString(), CharsetUtil.UTF_8));
	        	response.headers().set(CONTENT_TYPE, "text/html; charset=UTF-8");
        	}
        }
        keepAlive=false;
        if (keepAlive) {
            // Add 'Content-Length' header only for a keep-alive connection.
            response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
            // Add keep alive header as per:
            // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
            response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }else{
        	response.headers().set(CONNECTION, HttpHeaders.Values.CLOSE);
        }

        // Encode the cookie.
        String cookieString = request.headers().get(COOKIE);
        if (cookieString != null) {
            Set<Cookie> cookies = CookieDecoder.decode(cookieString);
            if (!cookies.isEmpty()) {
                // Reset the cookies if necessary.
                for (Cookie cookie: cookies) {
                    response.headers().add(SET_COOKIE, ServerCookieEncoder.encode(cookie));
                }
            }
        } else {
            // Browser sent no cookie.  Add some.
            //response.headers().add(SET_COOKIE, ServerCookieEncoder.encode("key1", "value1"));
            //response.headers().add(SET_COOKIE, ServerCookieEncoder.encode("key2", "value2"));
        }

        // Write the response.
        ChannelFuture f = ctx.writeAndFlush(response);
        f.addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				((SocketChannel)future.channel()).close();
			}
		});
        return keepAlive;
    }

    private static void send100Continue(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE);
        ctx.write(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}