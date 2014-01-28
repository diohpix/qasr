package com.cj.enm.platform.dbproxy.cache.redis;


public class RedisException extends RuntimeException {
	
	private static final long serialVersionUID = -284436474754492458L;

	public RedisException(String msg) {
		super(msg);
	}
}