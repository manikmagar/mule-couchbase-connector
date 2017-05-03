package org.mule.modules.couchbase.automation.functional;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mule.modules.couchbase.automation.runner.CouchbaseAbstractTestCase;
import org.mule.modules.couchbase.model.CbMapDocument;

import com.couchbase.client.java.error.DocumentDoesNotExistException;

public class UpdateDocumentTestCases extends CouchbaseAbstractTestCase {

	@Test(expected=DocumentDoesNotExistException.class)
	public void testNonExistantDocumentException() {
		org.mule.api.MuleEvent muleEvent = null;
		
		CbMapDocument cbMapDocument = new CbMapDocument();
		cbMapDocument.setId("non-existant-user");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","non-existant-user");
		cbMapDocument.setContent(content);
		
		getConnector().updateDocument(muleEvent, cbMapDocument);
	}

	@Test
	public void testDocumentUpdate() {
		org.mule.api.MuleEvent muleEvent = null;
		
		CbMapDocument cbMapDocument = new CbMapDocument();
		cbMapDocument.setId("user1");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","MyName");
		cbMapDocument.setContent(content);
		
		CbMapDocument returnDoc = getConnector().updateDocument(muleEvent, cbMapDocument);
		
		assertNotNull(returnDoc.getCas());
		//Loaded document has state but when we replace, it should not be there
		assertNull(returnDoc.getContent().get("state"));
		assertEquals(returnDoc.getId(), "user1");
		assertEquals(returnDoc.getContent().get("name").toString(), "MyName");
	}
}