
package com.cj.enm.platform.dbproxy;

import java.util.concurrent.ThreadPoolExecutor;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.Modifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cj.enm.platform.dbproxy.protocol.server.APIServer;
import com.cj.enm.platform.dbproxy.system.SystemInitializer;
import com.cj.enm.platform.dbproxy.util.CommonObject;

@SuppressWarnings("unused")
public class Main {
	final static Logger logger = LoggerFactory.getLogger(Main.class);
    private static APIServer server = null;
    public static void main(String args[]) throws Exception{
    	
    	CtClass string = ClassPool.getDefault().get("java.lang.String");
    	CtClass cc = ClassPool.getDefault().get("org.apache.ibatis.scripting.defaults.RawSqlSource");
    	CtField f = new CtField(string, "orgSql", cc);
    	f.setModifiers(Modifier.PUBLIC);
    	cc.addField(f);
    	CtConstructor []m = cc.getConstructors();
    	for (CtConstructor ctConstructor : m) {
			if(ctConstructor.toString().indexOf("[public RawSqlSource (Lorg/apache/ibatis/session/Configuration;Ljava/lang/String;Ljava/lang/Class;)V]")>0){
				ctConstructor.insertAfter("{this.orgSql=sql.trim();}");
				System.out.println("OK");
			}
		}
    	cc.toClass();
    	//m.insertBefore("{ System.out.println($1); System.out.println($2); }");
    	//cc.writeFile();
    	
    	SystemInitializer.initConfigure(args);
    	SystemInitializer.initDataSource();
        try{
	        server = SystemInitializer.initAPIServer();
	        //SystemInitializer.initMonitorServer();
	        
	        logger.info("Context Listener > Initialized");
	        
	        
	        /*
				SqlSessionFactory 	f = Configure.getSQLFactory("comment");
				Collection<MappedStatement> s = f.getConfiguration().getMappedStatements();
				for (MappedStatement mappedStatement : s) {
					BoundSql sql = mappedStatement.getBoundSql(null);
					EDbVendor vendor = EDbVendor.dbvmysql;
					System.out.println("scan");
					scantable scan = new scantable( sql.getSql(), vendor );
					System.out.print( scan.getScanResult( ) );
					System.out.println(sql.getSql());
					System.out.println("-------------------------------------------------");
				}*/
			//MapperBuilderAssistant mass = new MapperBuilderAssistant(proxy.getObject().getConfiguration(), null);

        }catch(Exception e){
        	if(server!=null){
        		server.shutdown();
        	}
        	if(CommonObject.executor!=null){
        		((ThreadPoolExecutor)CommonObject.executor).shutdown();
        	}
        	logger.error(e.getCause().getMessage(), e);
        	e.printStackTrace();
        	System.exit(-1);
        }
    }
    
    public void destroy() {
    	if(server!=null){
    		server.shutdown();
    	}
    	if(CommonObject.executor!=null){
    		((ThreadPoolExecutor)CommonObject.executor).shutdown();
    	}
    	logger.info("Complete");
    }
}
