package me.interest.pi.relay.util;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.context.ApplicationContext;

public class CommonObject {
	public static Executor executor=null;
    public static ApplicationContext  context=null;
    public static LinkedBlockingQueue<Runnable> queue=null;
    
}
