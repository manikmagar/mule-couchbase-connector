package org.mule.modules.couchbase.automation.runner;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mule.modules.couchbase.CouchbaseConnector;
import org.mule.tools.devkit.ctf.mockup.ConnectorTestContext;

//@RunWith(Suite.class)
//@SuiteClasses({
//
//GreetTestCases.class
//})

public class FunctionalTestSuite {
	
	@BeforeClass
	public static void initialiseSuite(){
		ConnectorTestContext.initialize(CouchbaseConnector.class);
	}
	
	@AfterClass
    public static void shutdownSuite() {
    	ConnectorTestContext.shutDown();
    }
	
}