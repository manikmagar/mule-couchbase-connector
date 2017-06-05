package org.mule.modules.couchbase.automation.functional;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mule.modules.couchbase.automation.runner.CouchbaseAbstractTestCase;
import org.mule.modules.couchbase.model.JavaMapDocument;

import com.couchbase.client.java.error.DocumentDoesNotExistException;

public class DeleteDocumentTestCases extends CouchbaseAbstractTestCase {

	@Test
	public void testNonExistingDocumentDeleteFailure(){
		org.mule.api.MuleEvent muleEvent = null;
		
		JavaMapDocument javaMapDocument = new JavaMapDocument();
		javaMapDocument.setId("user6");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","MyName");
		javaMapDocument.setContent(content);
		
		boolean result = getConnector().deleteDocument(muleEvent, javaMapDocument);
		assertEquals(result, Boolean.FALSE);
	}
	
	@Test
	public void testNonExistingDocumentDelete(){
		org.mule.api.MuleEvent muleEvent = null;
		
		JavaMapDocument javaMapDocument = new JavaMapDocument();
		javaMapDocument.setId("user1");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","User1");
		javaMapDocument.setContent(content);
		
		boolean result = getConnector().deleteDocument(muleEvent, javaMapDocument);
		assertEquals(result, Boolean.TRUE);
	}

}