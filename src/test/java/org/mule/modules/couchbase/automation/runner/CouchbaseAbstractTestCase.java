package org.mule.modules.couchbase.automation.runner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.couchbase.mock.DocumentLoader;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mule.modules.couchbase.CouchbaseConnector;
import org.mule.tools.devkit.ctf.junit.AbstractTestCase;

public abstract class CouchbaseAbstractTestCase extends AbstractTestCase<CouchbaseConnector> {

	public CouchbaseAbstractTestCase() {
		super(CouchbaseConnector.class);
	}

	@Before
	public void flushBucket() throws FileNotFoundException, IOException{
		FunctionalTestSuite.bucket.bucketManager().flush();

		DocumentLoader.loadFromSerializedXZ(new FileInputStream("./src/test/resources/user-sample-data.serialized.xz"),
				"default", FunctionalTestSuite.couchbaseMock);
	}
	
	
	public void assertNotNull(Object value){
		MatcherAssert.assertThat(value, Matchers.notNullValue());
	}
	

	public void assertNull(Object value){
		MatcherAssert.assertThat(value, Matchers.nullValue());
	}
	
	
	public <T> void assertEquals(T actual, T expected){
		MatcherAssert.assertThat(actual, Matchers.equalTo(expected));
	}
	
}
