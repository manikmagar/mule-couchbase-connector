package org.mule.modules.couchbase.automation.functional;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mule.modules.couchbase.automation.runner.CouchbaseAbstractTestCase;
import org.mule.modules.couchbase.model.JavaMapDocument;

import com.couchbase.client.java.error.DocumentAlreadyExistsException;

public class InsertDocumentTestCases extends CouchbaseAbstractTestCase {

	@Test(expected=DocumentAlreadyExistsException.class)
	public void testExistingDocumentInsertFailure(){
		org.mule.api.MuleEvent muleEvent = null;
		
		JavaMapDocument javaMapDocument = new JavaMapDocument();
		javaMapDocument.setId("user1");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","MyName");
		javaMapDocument.setContent(content);
		
		getConnector().insertDocument(muleEvent, javaMapDocument);
		
	}
	
	@Test
	public void testDocumentInsert() {
		org.mule.api.MuleEvent muleEvent = null;
		
		JavaMapDocument javaMapDocument = new JavaMapDocument();
		javaMapDocument.setId("user6");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","MyName");
		content.put("state","CA");
		javaMapDocument.setContent(content);
		
		JavaMapDocument returnDoc = getConnector().insertDocument(muleEvent, javaMapDocument);
		
		assertNotNull(returnDoc.getCas());
		assertEquals(returnDoc.getId(), "user6");
		assertEquals(returnDoc.getContent().get("name").toString(), "MyName");
		assertEquals(returnDoc.getContent().get("state").toString(), "CA");
	}

}