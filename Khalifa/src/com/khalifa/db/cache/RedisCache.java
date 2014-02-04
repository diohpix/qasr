package com.khalifa.db.cache;



import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.CacheException;
import org.apache.ibatis.cache.decorators.LoggingCache;
import org.apache.ibatis.cache.decorators.SerializedCache;
import org.apache.ibatis.cache.impl.PerpetualCache;

public class RedisCache implements Cache {

  private SerializedCache scache;
  private String id;


  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  public RedisCache(){
	  this.scache =  new SerializedCache(new LoggingCache(new PerpetualCache(id)));
  }
  public RedisCache(String id) {
    this.id = id;
    this.scache =  new SerializedCache(new LoggingCache(new PerpetualCache(id)));
  }

  public String getId() {
    return id;
  }

  public int getSize() {
    return scache.getSize();
  }

  public void putObject(Object key, Object value) {
	  System.out.println("PUT "+key);
    scache.putObject(key, value);
  }

  public Object getObject(Object key) {
	  System.out.println("GET "+key);
    return scache.getObject(key);
  }

  public Object removeObject(Object key) {
	 System.out.println("REMOVE "+key);
    return scache.removeObject(key);
  }

  public void clear() {
	 System.out.println("CLEAR");
    scache.clear();
  }

  public ReadWriteLock getReadWriteLock() {
    return readWriteLock;
  }

    
  public boolean equals(Object o) {
    if (getId() == null) throw new CacheException("Cache instances require an ID.");
    if (this == o) return true;
    if (!(o instanceof Cache)) return false;

    Cache otherCache = (Cache) o;
    return getId().equals(otherCache.getId());
  }

  public int hashCode() {
    if (getId() == null) throw new CacheException("Cache instances require an ID.");
    return getId().hashCode();
  }

}
