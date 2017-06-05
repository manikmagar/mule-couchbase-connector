package org.mule.modules.couchbase.automation.runner;

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
import org.couchbase.mock.JsonUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.mule.modules.couchbase.CouchbaseConnector;
import org.mule.modules.couchbase.automation.functional.DeleteDocumentTestCases;
import org.mule.modules.couchbase.automation.functional.GetDocumentTestCases;
import org.mule.modules.couchbase.automation.functional.InsertDocumentTestCases;
import org.mule.modules.couchbase.automation.functional.UnlockDocumentTestCases;
import org.mule.modules.couchbase.automation.functional.UpdateDocumentTestCases;
import org.mule.modules.couchbase.automation.functional.UpsertDocumentTestCases;
import org.mule.modules.couchbase.config.CouchbaseClusterClient;
import org.mule.modules.couchbase.config.CouchbaseConnectorConfig;
import org.mule.tools.devkit.ctf.mockup.ConnectorTestContext;

import com.couchbase.client.java.Cluster;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

@RunWith(Suite.class)
@SuiteClasses({

GetDocumentTestCases.class,
UpdateDocumentTestCases.class,
InsertDocumentTestCases.class,
UpsertDocumentTestCases.class,
DeleteDocumentTestCases.class,
UnlockDocumentTestCases.class
})

public class FunctionalTestSuite {

	public static CouchbaseMock couchbaseMock;
	public static com.couchbase.client.java.Bucket bucket;

	protected static final BucketConfiguration bucketConfiguration = new BucketConfiguration();

	protected static Cluster cluster;

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
		ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
		configList.add(bucketConfiguration);
		couchbaseMock = new CouchbaseMock(0, configList);
		couchbaseMock.start();
		couchbaseMock.waitForStartup();

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
	public static void initialiseSuite() throws Exception{

		createMock("default", "");
		getPortInfo("default");
		createClient();
		ConnectorTestContext.initialize(CouchbaseConnector.class);
	}

	@AfterClass
    public static void shutdownSuite() {
		if (cluster != null) {
			System.out.println("Stop cluster");
			bucket.close();
			cluster.disconnect();
		}
		if(client.isConnected()){
			System.out.println("Disconnect client");
			client.disconnectCluster();
		}
		if (couchbaseMock != null) {
			System.out.println("Stop mock");
			couchbaseMock.stop();
		}
    	ConnectorTestContext.shutDown();
    }


}
