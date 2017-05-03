package org.mule.modules.couchbase.automation.functional;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mule.modules.couchbase.automation.runner.CouchbaseAbstractTestCase;
import org.mule.modules.couchbase.model.CbMapDocument;

public class UpsertDocumentTestCases extends CouchbaseAbstractTestCase {

	@Test
	public void testNonExistantDocumentInsert() {
		org.mule.api.MuleEvent muleEvent = null;
		
		CbMapDocument cbMapDocument = new CbMapDocument();
		cbMapDocument.setId("user6");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","MyName");
		content.put("state","CA");
		cbMapDocument.setContent(content);
		
		CbMapDocument returnDoc = getConnector().upsertDocument(muleEvent, cbMapDocument);
		
		assertNotNull(returnDoc.getCas());
		assertEquals(returnDoc.getId(), "user6");
		assertEquals(returnDoc.getContent().get("name").toString(), "MyName");
		assertEquals(returnDoc.getContent().get("state").toString(), "CA");
	}
	
	@Test
	public void testExistingDocumentUpdate() {
		org.mule.api.MuleEvent muleEvent = null;
		
		CbMapDocument cbMapDocument = new CbMapDocument();
		cbMapDocument.setId("user1");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","MyName");
		cbMapDocument.setContent(content);
		
		CbMapDocument returnDoc = getConnector().upsertDocument(muleEvent, cbMapDocument);
		
		assertNotNull(returnDoc.getCas());
		//Loaded document has state but when we replace, it should not be there
		assertNull(returnDoc.getContent().get("state"));
		assertEquals(returnDoc.getId(), "user1");
		assertEquals(returnDoc.getContent().get("name").toString(), "MyName");
	}

}