package org.mule.modules.couchbase.config;

import javax.validation.constraints.NotNull;

import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Connect;
import org.mule.api.annotations.ConnectionIdentifier;
import org.mule.api.annotations.Disconnect;
import org.mule.api.annotations.TestConnectivity;
import org.mule.api.annotations.ValidateConnection;
import org.mule.api.annotations.components.ConnectionManagement;
import org.mule.api.annotations.display.FriendlyName;
import org.mule.api.annotations.display.Password;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Default;
import org.mule.util.StringUtils;

import com.couchbase.client.java.Bucket;

@ConnectionManagement(friendlyName = "Configuration", configElementName="couchbase-config")
public class CouchbaseConnectorConfig {
    
	@Configurable
	@Default("default")
	private String bucketName;
	
	@Configurable
	@Password
	@Default("")
	private String password;
	
	@Configurable
	@Default("8091")
	@NotNull
	@FriendlyName("HTTP Non-Encrypted Port")
	private int bootstrapHttpDirectPort;
	
	@Configurable
	@Default("11210")
	@NotNull
	@FriendlyName("Carrier Non-Encrypted Port")
	private int bootstrapCarrierDirectPort;
	
    public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
	
	public String getPassword() {
		return password;
	}

	public int getBootstrapHttpDirectPort() {
		return bootstrapHttpDirectPort;
	}

	public void setBootstrapHttpDirectPort(int bootstrapHttpDirectPort) {
		this.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
	}

	public int getBootstrapCarrierDirectPort() {
		return bootstrapCarrierDirectPort;
	}

	public void setBootstrapCarrierDirectPort(int bootstrapCarrierDirectPort) {
		this.bootstrapCarrierDirectPort = bootstrapCarrierDirectPort;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	private CouchbaseClusterClient cbClusterClient = CouchbaseClusterClient.get();
	
	public Bucket openBucket(){
		return cbClusterClient.openBucket(getBucketName(), getPassword());
	}
	
	@Connect
    @TestConnectivity
    public void connect(@ConnectionKey @Default("localhost") String clusterSeedNodes)
        throws ConnectionException {
		
        try {
        	cbClusterClient.createCluster(clusterSeedNodes, this);
        	if(!StringUtils.isEmpty(getBucketName())){
        		cbClusterClient.openBucket(getBucketName(), getPassword());
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
    	cbClusterClient.disconnectCluster();
    }

    /**
     * Are we connected
     */
    @ValidateConnection
    public boolean isConnected() {
       return cbClusterClient.isConnected();
    }

    /**
     * Are we connected
     */
    @ConnectionIdentifier
    public String connectionId() {
        return "001";
    }
    
}