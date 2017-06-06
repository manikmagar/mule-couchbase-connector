/**
 * The software in this package is published under the terms of the Apache v2.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.couchbase.automation.functional;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mule.modules.couchbase.automation.runner.CouchbaseAbstractTestCase;
import org.mule.modules.couchbase.model.JavaMapDocument;

public class UpsertDocumentTestCases extends CouchbaseAbstractTestCase {

	@Test
	public void testNonExistantDocumentInsert() {
		org.mule.api.MuleEvent muleEvent = null;
		
		JavaMapDocument javaMapDocument = new JavaMapDocument();
		javaMapDocument.setId("user6");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","MyName");
		content.put("state","CA");
		javaMapDocument.setContent(content);
		
		JavaMapDocument returnDoc = getConnector().upsertDocument(muleEvent, javaMapDocument);
		
		assertNotNull(returnDoc.getCas());
		assertEquals(returnDoc.getId(), "user6");
		assertEquals(returnDoc.getContent().get("name").toString(), "MyName");
		assertEquals(returnDoc.getContent().get("state").toString(), "CA");
	}
	
	@Test
	public void testExistingDocumentUpdate() {
		org.mule.api.MuleEvent muleEvent = null;
		
		JavaMapDocument javaMapDocument = new JavaMapDocument();
		javaMapDocument.setId("user1");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","MyName");
		javaMapDocument.setContent(content);
		
		JavaMapDocument returnDoc = getConnector().upsertDocument(muleEvent, javaMapDocument);
		
		assertNotNull(returnDoc.getCas());
		//Loaded document has state but when we replace, it should not be there
		assertNull(returnDoc.getContent().get("state"));
		assertEquals(returnDoc.getId(), "user1");
		assertEquals(returnDoc.getContent().get("name").toString(), "MyName");
	}

}