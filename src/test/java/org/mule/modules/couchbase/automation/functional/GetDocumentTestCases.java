/**
 * The software in this package is published under the terms of the Apache v2.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.couchbase.automation.functional;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mule.modules.couchbase.automation.runner.CouchbaseAbstractTestCase;
import org.mule.modules.couchbase.model.JavaMapDocument;

import com.couchbase.client.java.error.CASMismatchException;

public class GetDocumentTestCases extends CouchbaseAbstractTestCase {


	@Test
	public void verifyNoDocumentReturn() {
		org.mule.api.MuleEvent muleEvent = null;
		java.lang.String id = "1";
		boolean lockDocument = false;
		int lockDuration = 0;
		boolean refreshExpirationTime = false;
		int refreshTime = 60;
		JavaMapDocument returnDoc = getConnector().getDocument(muleEvent, id, lockDocument, lockDuration, refreshExpirationTime,
				refreshTime);
		MatcherAssert.assertThat(returnDoc, Matchers.nullValue());
		
	}
	
	@Test
	public void verifyDocumentReturn() {
		org.mule.api.MuleEvent muleEvent = null;
		java.lang.String id = "user1";
		boolean lockDocument = false;
		int lockDuration = 0;
		boolean refreshExpirationTime = false;
		int refreshTime = 60;
		JavaMapDocument returnDoc = getConnector().getDocument(muleEvent, id, lockDocument, lockDuration, refreshExpirationTime,
				refreshTime);
		MatcherAssert.assertThat(returnDoc, Matchers.notNullValue());
		MatcherAssert.assertThat(returnDoc.getCas(), Matchers.notNullValue());
		MatcherAssert.assertThat(returnDoc.getId(), Matchers.equalTo("user1"));
		MatcherAssert.assertThat(returnDoc.getContent().get("state").toString(), Matchers.equalTo("DE"));
		
	}
	
	/**
	 * Tests the Pessimistic locking of a document. Locked document can only be updated if CAS matches with the value stored in server.
	 * Incorrect CAS value in update should result in exception.
	 */
	@Test
	public void verifyDocumentLockException() {
		org.mule.api.MuleEvent muleEvent = null;
		java.lang.String id = "user1";
		boolean lockDocument = true;
		int lockDuration = 15;
		boolean refreshExpirationTime = false;
		int refreshTime = 60;
		JavaMapDocument returnDoc = getConnector().getDocument(muleEvent, id, lockDocument, lockDuration, refreshExpirationTime,
				refreshTime);
		
		//Change the CAS and update the object. It should throw an exception.
		
		JavaMapDocument newDoc = returnDoc.clone();
		
		newDoc.setCas(1l);
		newDoc.getContent().put("city", "anything");
		
		boolean asExpected = false;
		
		try {

			getConnector().updateDocument(muleEvent, newDoc);
			
		} catch (CASMismatchException e) {
			asExpected = true;
			getConnector().unlockDocument(muleEvent, returnDoc.getId(), returnDoc.getCas());
		}

		MatcherAssert.assertThat(asExpected, Matchers.equalTo(Boolean.TRUE));
	}
	
	/**
	 * Tests the Pessimistic locking of a document. Locked document can only be updated if CAS matches with the value stored in server.
	 * Incorrect CAS value in update should result in exception.
	 */
	@Test
	public void verifyDocumentLockNoException() {
		org.mule.api.MuleEvent muleEvent = null;
		java.lang.String id = "user1";
		boolean lockDocument = true;
		int lockDuration = 15;
		boolean refreshExpirationTime = false;
		int refreshTime = 60;
		JavaMapDocument returnDoc = getConnector().getDocument(muleEvent, id, lockDocument, lockDuration, refreshExpirationTime,
				refreshTime);
		//Change an attribute, keeping CAS same.
		returnDoc.getContent().put("city", "anything");
	
		JavaMapDocument returnDoc2 = getConnector().updateDocument(muleEvent, returnDoc);
			
		MatcherAssert.assertThat(returnDoc2.getContent().get("city").toString(), Matchers.equalTo("anything"));
	}

}