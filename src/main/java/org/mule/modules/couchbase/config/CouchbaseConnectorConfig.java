package org.mule.modules.couchbase.config;

import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Connect;
import org.mule.api.annotations.ConnectionIdentifier;
import org.mule.api.annotations.Disconnect;
import org.mule.api.annotations.TestConnectivity;
import org.mule.api.annotations.ValidateConnection;
import org.mule.api.annotations.components.ConnectionManagement;
import org.mule.api.annotations.display.Password;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Default;
import org.mule.util.StringUtils;

import com.couchbase.client.java.Bucket;

@ConnectionManagement(friendlyName = "Configuration")
public class CouchbaseConnectorConfig {
    
	@Configurable
	@Default("default")
	private String bucketName;
	
	@Configurable
	@Password
	@Default("")
	private String password;
	
    public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
	
	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	private CouchbaseConnectionUtil couchbaseConnection = CouchbaseConnectionUtil.get();
	
	public Bucket openBucket(){
		return couchbaseConnection.openBucket(getBucketName(), getPassword());
	}
	
	@Connect
    @TestConnectivity
    public void connect(@ConnectionKey @Default("localhost") String clusterSeedNodes)
        throws ConnectionException {
		
        try {
        	couchbaseConnection.createCluster(clusterSeedNodes);
        	if(!StringUtils.isEmpty(getBucketName())){
        		couchbaseConnection.openBucket(getBucketName(), getPassword());
        	}
		} catch (Exception e) {
			throw new ConnectionException(ConnectionExceptionCode.CANNOT_REACH,"","Unable to connect to couchbase cluster or open specified bucket");
		}
    }

    /**
     * Disconnect
     */
    @Disconnect
    public void disconnect() {
    	couchbaseConnection.disconnectCluster();
    }

    /**
     * Are we connected
     */
    @ValidateConnection
    public boolean isConnected() {
       return couchbaseConnection.isConnected();
    }

    /**
     * Are we connected
     */
    @ConnectionIdentifier
    public String connectionId() {
        return "001";
    }
    
}