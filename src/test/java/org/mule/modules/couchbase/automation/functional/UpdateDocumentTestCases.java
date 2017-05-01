package org.mule.modules.couchbase.automation.functional;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mule.modules.couchbase.automation.runner.CouchbaseAbstractTestCase;
import org.mule.modules.couchbase.model.CbMapDocument;

public class UpdateDocumentTestCases extends CouchbaseAbstractTestCase {

	@Test
	public void verify() {
		org.mule.modules.couchbase.model.CbMapDocument expected = null;
		org.mule.api.MuleEvent muleEvent = null;
		
		CbMapDocument cbMapDocument = new CbMapDocument();
		cbMapDocument.setId("non-existant-user");
		
		Map<String, Object> content = new HashMap<String, Object>();
		content.put("name","non-existant-user");
		cbMapDocument.setContent(content);
		
		getConnector().updateDocument(muleEvent, cbMapDocument);
	}

}