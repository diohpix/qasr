	package com.khalifa.protocol.server.protobuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.khalifa.process.State;

import io.netty.util.AttributeKey;

public class CommonData {
	public static final AttributeKey<State> STATE =  AttributeKey.valueOf("request.state");
	public final static Logger timeoutLogger = LoggerFactory.getLogger("TIMEOUT");
	public final static Logger exceptionLogger = LoggerFactory.getLogger("EXCEPTION");
	

}
