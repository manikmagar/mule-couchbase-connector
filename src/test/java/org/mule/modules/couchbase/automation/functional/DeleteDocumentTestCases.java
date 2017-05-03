package org.mule.modules.couchbase.automation.functional;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mule.modules.couchbase.automation.runner.CouchbaseAbstractTestCase;
import org.mule.modules.couchbase.model.CbMapDocument;

import com.couchbase.client.java.error.DocumentDoesNotExistException;

public class DeleteDocumentTestCases extends CouchbaseAbstractTestCase {

	@Test(expected=DocumentDoesNotExistException.class)
	public void testNonExistingDocumentDeleteFailure(){
		org.mule.api.MuleEvent muleEvent = null;
		
		CbMapDocument cbMapDocument = new CbMapDocument();
		cbMapDocument.setId("user6");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","MyName");
		cbMapDocument.setContent(content);
		
		boolean result = getConnector().deleteDocument(muleEvent, cbMapDocument);
		assertEquals(result, Boolean.FALSE);
	}
	
	@Test
	public void testNonExistingDocumentDelete(){
		org.mule.api.MuleEvent muleEvent = null;
		
		CbMapDocument cbMapDocument = new CbMapDocument();
		cbMapDocument.setId("user1");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","User1");
		cbMapDocument.setContent(content);
		
		boolean result = getConnector().deleteDocument(muleEvent, cbMapDocument);
		assertEquals(result, Boolean.TRUE);
	}

}