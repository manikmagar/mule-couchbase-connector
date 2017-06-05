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
import org.mule.api.annotations.display.FriendlyName;
import org.mule.api.annotations.display.Password;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.display.Summary;
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
	@Default("false")
	@Placement(group="SSL Connection",order=2,tab="Couchbase Environment")
	@FriendlyName("SSL Enabled?")
	@Summary("Requires Couchbase v3+ Enterprise Edition Cluster")
	private boolean sslEnabled;
	
	@Configurable
	@Default("")
	@Placement(group="SSL Connection",order=2,tab="Couchbase Environment")
	@FriendlyName("SSL Keystore File path")
	@Summary("SSL Must be enabled.")
	private  String sslKeystoreFile;
	
	@Configurable
	@Default("")
	@Password
	@Placement(group="SSL Connection",order=2,tab="Couchbase Environment")
	@FriendlyName("SSL Keystore Password")
	@Summary("SSL Must be enabled.")
	private  String sslKeystorePassword;
	

	@Configurable
	@Default("true")
	@FriendlyName("Config Loading through HTTP")
	@Placement(group="Connection Ports",order=1,tab="Couchbase Environment")
	private  boolean bootstrapHttpEnabled = true;

	
	@Configurable
	@Placement(group="Connection Ports",order=1,tab="Couchbase Environment")
	@Default("8091")
	@FriendlyName("HTTP Non-Encrypted Port")
	private  int bootstrapHttpDirectPort;
	
	@Configurable
	@Placement(group="Connection Ports",order=1,tab="Couchbase Environment")
	@Default("18091")
	@FriendlyName("HTTP Encrypted Port")
	@Summary("SSL must be enabled for this to use.")
	private  int bootstrapHttpSslPort;
	
	
	@Configurable
	@Default("true")
	@FriendlyName("Enable Config Loading through Carrier Publication")
	@Placement(group="Connection Ports",order=1,tab="Couchbase Environment")
	private  boolean bootstrapCarrierEnabled = true;
	
	@Configurable
	@Placement(group="Connection Ports",order=1,tab="Couchbase Environment")
	@Default("11210")
	@FriendlyName("Carrier Non-Encrypted Port")
	private  int bootstrapCarrierDirectPort;
	
	@Configurable
	@Placement(group="Connection Ports",order=1,tab="Couchbase Environment")
	@Default("11207")
	@FriendlyName("Carrier Encrypted Port")
	@Summary("SSL must be enabled for this to use.")
	private  int bootstrapCarrierSslPort;
	
	public boolean isSslEnabled() {
		return sslEnabled;
	}
	public void setSslEnabled(boolean sslEnabled) {
		this.sslEnabled = sslEnabled;
	}
	public String getSslKeystoreFile() {
		return sslKeystoreFile;
	}
	public void setSslKeystoreFile(String sslKeystoreFile) {
		this.sslKeystoreFile = sslKeystoreFile;
	}
	public String getSslKeystorePassword() {
		return sslKeystorePassword;
	}
	public void setSslKeystorePassword(String sslKeystorePassword) {
		this.sslKeystorePassword = sslKeystorePassword;
	}
	public boolean isBootstrapHttpEnabled() {
		return bootstrapHttpEnabled;
	}
	public void setBootstrapHttpEnabled(boolean bootstrapHttpEnabled) {
		this.bootstrapHttpEnabled = bootstrapHttpEnabled;
	}
	public boolean isBootstrapCarrierEnabled() {
		return bootstrapCarrierEnabled;
	}
	public void setBootstrapCarrierEnabled(boolean bootstrapCarrierEnabled) {
		this.bootstrapCarrierEnabled = bootstrapCarrierEnabled;
	}
	public int getBootstrapHttpDirectPort() {
		return bootstrapHttpDirectPort;
	}
	public void setBootstrapHttpDirectPort(int bootstrapHttpDirectPort) {
		this.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
	}
	public int getBootstrapHttpSslPort() {
		return bootstrapHttpSslPort;
	}
	public void setBootstrapHttpSslPort(int bootstrapHttpSslPort) {
		this.bootstrapHttpSslPort = bootstrapHttpSslPort;
	}
	public int getBootstrapCarrierDirectPort() {
		return bootstrapCarrierDirectPort;
	}
	public void setBootstrapCarrierDirectPort(int bootstrapCarrierDirectPort) {
		this.bootstrapCarrierDirectPort = bootstrapCarrierDirectPort;
	}
	public int getBootstrapCarrierSslPort() {
		return bootstrapCarrierSslPort;
	}
	public void setBootstrapCarrierSslPort(int bootstrapCarrierSslPort) {
		this.bootstrapCarrierSslPort = bootstrapCarrierSslPort;
	}
	
	
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