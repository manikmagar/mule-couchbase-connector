package org.mule.modules.couchbase.automation.functional;

import org.junit.Test;
import org.mule.modules.couchbase.automation.runner.CouchbaseAbstractTestCase;
import org.mule.modules.couchbase.model.JavaMapDocument;

import com.couchbase.client.java.error.DocumentDoesNotExistException;

public class UnlockDocumentTestCases extends CouchbaseAbstractTestCase {

	@Test(expected=DocumentDoesNotExistException.class)
	public void testNonExistantDocumentUnlock(){
		
		Boolean result = getConnector().unlockDocument(null, "user8", 0l);
		
		assertEquals(result, Boolean.FALSE);
		
	}
	
	@Test
	public void testDocumentUnlock(){
		
		JavaMapDocument inputDoc = getConnector().getDocument(null, "user1", true, 15, false, 0);
		
		Boolean result = getConnector().unlockDocument(null, inputDoc.getId(), inputDoc.getCas());
		
		assertEquals(result, Boolean.TRUE);
		
	}
}