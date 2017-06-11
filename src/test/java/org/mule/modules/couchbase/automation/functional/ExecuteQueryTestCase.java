package org.mule.modules.couchbase.automation.functional;

import org.junit.Test;
import org.mule.modules.couchbase.automation.runner.CouchbaseAbstractTestCase;

public class ExecuteQueryTestCase extends CouchbaseAbstractTestCase {

	
	@Test
	public void testExecuteQuery(){
		
		//CouchbaseMock does not support N1QL queries.
		
//		String query = "select * from default where type = 'user'";
//		org.mule.api.MuleEvent muleEvent = null;
//		
//		List<Map<String, Object>> lst = getConnector().executeQuery(muleEvent, query, Collections.EMPTY_LIST);
//		
//		assertNotNull(lst);
//		
	}
	
}
