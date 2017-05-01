package org.mule.modules.couchbase.automation.runner;

import java.io.FileInputStream;
import java.util.ArrayList;

import javax.validation.constraints.NotNull;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.couchbase.mock.BucketConfiguration;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.DocumentLoader;
import org.couchbase.mock.JsonUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mule.modules.couchbase.CouchbaseConnector;
import org.mule.modules.couchbase.config.CouchbaseClusterClient;
import org.mule.modules.couchbase.config.CouchbaseConnectorConfig;
import org.mule.tools.devkit.ctf.junit.AbstractTestCase;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public abstract class CouchbaseAbstractTestCase extends AbstractTestCase<CouchbaseConnector> {

	public CouchbaseAbstractTestCase() {
		super(CouchbaseConnector.class);
	}

	protected static final BucketConfiguration bucketConfiguration = new BucketConfiguration();
	protected static CouchbaseMock couchbaseMock;
	protected static Cluster cluster;
	protected static com.couchbase.client.java.Bucket bucket;
	protected static int carrierPort;
	protected static int httpPort;
	protected static CouchbaseClusterClient client;

	protected static void getPortInfo(String bucket) throws Exception {
		httpPort = couchbaseMock.getHttpPort();
		URIBuilder builder = new URIBuilder();
		builder.setScheme("http").setHost("localhost").setPort(httpPort).setPath("mock/get_mcports")
				.setParameter("bucket", bucket);
		HttpGet request = new HttpGet(builder.build());
		HttpClient client = HttpClientBuilder.create().build();
		HttpResponse response = client.execute(request);
		int status = response.getStatusLine().getStatusCode();
		if (status < 200 || status > 300) {
			throw new ClientProtocolException("Unexpected response status: " + status);
		}
		String rawBody = EntityUtils.toString(response.getEntity());
		JsonObject respObject = JsonUtils.GSON.fromJson(rawBody, JsonObject.class);
		JsonArray portsArray = respObject.getAsJsonArray("payload");
		carrierPort = portsArray.get(0).getAsInt();
	}

	protected static void createMock(@NotNull String name, @NotNull String password) throws Exception {
		bucketConfiguration.numNodes = 1;
		bucketConfiguration.numReplicas = 1;
		bucketConfiguration.numVBuckets = 1024;
		bucketConfiguration.name = name;
		bucketConfiguration.type = org.couchbase.mock.Bucket.BucketType.COUCHBASE;
		bucketConfiguration.password = password;
//		bucketConfiguration.bucketStartPort = 11210;
		ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
		configList.add(bucketConfiguration);
		couchbaseMock = new CouchbaseMock(0, configList);
		couchbaseMock.start();
		couchbaseMock.waitForStartup();

		DocumentLoader.loadFromSerializedXZ(new FileInputStream("./src/test/resources/user-sample-data.serialized.xz"),
				"default", couchbaseMock);
	}

	protected static void createClient() {
		client = CouchbaseClusterClient.get();
		CouchbaseConnectorConfig config = new CouchbaseConnectorConfig();
		config.setBootstrapHttpDirectPort(httpPort);
		config.setBootstrapCarrierDirectPort(carrierPort);
		cluster = client.createCluster("127.0.0.1", config);
		bucket = cluster.openBucket("default");
	}

	@BeforeClass
	public static void setupMock() throws Exception {
		
		createMock("default", "");
		getPortInfo("default");
		createClient();
	}

	@AfterClass
	public static void disconnectMock() throws Exception {
		if (cluster != null) {
			System.out.println("Stop cluster");
			bucket.close();
			cluster.disconnect();
		}
		if(client.isConnected()){
			client.disconnectCluster();
		}
		if (couchbaseMock != null) {
			System.out.println("Stop mock");
			couchbaseMock.stop();
		}

	}

}
