package org.mule.modules.couchbase.config;

import java.util.HashMap;
import java.util.Map;

import org.mule.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;

public class CouchbaseConnectionUtil {
	
	private Logger LOG = LoggerFactory.getLogger(CouchbaseConnectionUtil.class);
	
	private static final class SingleConnectionHolder {
		public static final CouchbaseConnectionUtil INSTANCE = new CouchbaseConnectionUtil();
	}
	
	private CouchbaseConnectionUtil(){
		
	}
	
	public static CouchbaseConnectionUtil get(){
		return SingleConnectionHolder.INSTANCE;
	}
	
	private Cluster couchbaseCluster;
	
	private Map<String, Bucket> couchbaseBuckets = new HashMap<String, Bucket>();
	
	public  Cluster createCluster(final String clusterSeedNodes){
		if(couchbaseCluster == null) {
			synchronized (SingleConnectionHolder.INSTANCE){
				if(couchbaseCluster == null) {
					LOG.debug("Instantiating new Couchbase Cluster");
					couchbaseCluster = CouchbaseCluster.create(clusterSeedNodes);
				}
			}
		}
		
		return couchbaseCluster;
	}
	
	public Bucket openBucket(final String bucketName, String password){
		if (!couchbaseBuckets.containsKey(bucketName)){
			synchronized (couchbaseBuckets){
				if (!couchbaseBuckets.containsKey(bucketName)){
					LOG.debug("Opening new bucket ?", bucketName);
					if(StringUtils.isEmpty(password)) password = (String) null;
					Bucket bucket = couchbaseCluster.openBucket(bucketName, password);
					couchbaseBuckets.put(bucketName, bucket);
				}
			}
			LOG.debug("Reusing already open bucket ?", bucketName);
		}
		return couchbaseBuckets.get(bucketName);
	}
	
	public  void disconnectCluster(){
		synchronized (couchbaseCluster){
			if (couchbaseCluster != null){
//				// Disconnect all open buckets
//				if(!couchbaseBuckets.isEmpty()){
//					for (String bucketName : couchbaseBuckets.keySet()) {
//						LOG.info("Closing Bucket ?", bucketName);
//						couchbaseBuckets.get(bucketName).close();
//					}
//				}
//				
				//Disconnect the cluster
				//This also internally closes all open buckets from this cluster
				couchbaseCluster.disconnect();
				couchbaseCluster = null;
				LOG.info("Disconnected from couchbase cluster");
			}
		}
	}
	
	public  boolean isConnected(){
		return couchbaseCluster != null;
	}

}
