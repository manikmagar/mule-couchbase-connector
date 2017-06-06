/**
 * The software in this package is published under the terms of the Apache v2.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.couchbase.config;

import java.util.HashMap;
import java.util.Map;

import org.mule.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

public class CouchbaseClusterClient {
	
	private Logger LOG = LoggerFactory.getLogger(CouchbaseClusterClient.class);
	
	private static final class SingleConnectionHolder {
		public static final CouchbaseClusterClient INSTANCE = new CouchbaseClusterClient();
	}
	
	private CouchbaseClusterClient(){
		
	}
	
	public static CouchbaseClusterClient get(){
		return SingleConnectionHolder.INSTANCE;
	}
	
	private Cluster couchbaseCluster;
	
	private Map<String, Bucket> couchbaseBuckets = new HashMap<String, Bucket>();
	
	public  Cluster createCluster(final String clusterSeedNodes, final CouchbaseConnectorConfig config){
		if(couchbaseCluster == null) {
			synchronized (SingleConnectionHolder.INSTANCE){
				if(couchbaseCluster == null) {
					LOG.debug("Instantiating new Couchbase Cluster");

	        		CouchbaseEnvironment env = DefaultCouchbaseEnvironment
	        				.builder()
	        				.bootstrapHttpDirectPort(config.getBootstrapHttpDirectPort())
	        				.bootstrapCarrierDirectPort(config.getBootstrapCarrierDirectPort())
	        				.bootstrapCarrierSslPort(config.getBootstrapCarrierSslPort())
	        				.bootstrapCarrierEnabled(config.isBootstrapCarrierEnabled())
	        				.bootstrapHttpEnabled(config.isBootstrapHttpEnabled())
	        				.bootstrapHttpSslPort(config.getBootstrapHttpSslPort())
	        				.sslEnabled(config.isSslEnabled())
	        				.sslKeystoreFile(config.getSslKeystoreFile())
	        				.sslKeystorePassword(config.getSslKeystorePassword())
	        				.build();
	        		
					couchbaseCluster = CouchbaseCluster.create(env,clusterSeedNodes);
				}
			}
		} else {
			LOG.debug("Using Existing Cluster");
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
