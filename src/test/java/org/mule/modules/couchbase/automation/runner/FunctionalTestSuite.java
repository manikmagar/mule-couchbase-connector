package org.mule.modules.couchbase.automation.runner;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.mule.modules.couchbase.CouchbaseConnector;
import org.mule.modules.couchbase.automation.functional.GetDocumentTestCases;
import org.mule.modules.couchbase.automation.functional.UpdateDocumentTestCases;
import org.mule.tools.devkit.ctf.mockup.ConnectorTestContext;

@RunWith(Suite.class)
@SuiteClasses({

GetDocumentTestCases.class,
UpdateDocumentTestCases.class
})

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