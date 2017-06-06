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

import com.couchbase.client.java.error.DocumentDoesNotExistException;

public class UpdateDocumentTestCases extends CouchbaseAbstractTestCase {

	@Test(expected=DocumentDoesNotExistException.class)
	public void testNonExistantDocumentException() {
		org.mule.api.MuleEvent muleEvent = null;
		
		JavaMapDocument javaMapDocument = new JavaMapDocument();
		javaMapDocument.setId("non-existant-user");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","non-existant-user");
		javaMapDocument.setContent(content);
		
		getConnector().updateDocument(muleEvent, javaMapDocument);
	}

	@Test
	public void testDocumentUpdate() {
		org.mule.api.MuleEvent muleEvent = null;
		
		JavaMapDocument javaMapDocument = new JavaMapDocument();
		javaMapDocument.setId("user1");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","MyName");
		javaMapDocument.setContent(content);
		
		JavaMapDocument returnDoc = getConnector().updateDocument(muleEvent, javaMapDocument);
		
		assertNotNull(returnDoc.getCas());
		//Loaded document has state but when we replace, it should not be there
		assertNull(returnDoc.getContent().get("state"));
		assertEquals(returnDoc.getId(), "user1");
		assertEquals(returnDoc.getContent().get("name").toString(), "MyName");
	}
}