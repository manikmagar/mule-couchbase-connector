package org.mule.modules.couchbase.automation.functional;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.couchbase.CouchbaseConnector;
import org.mule.modules.couchbase.model.CbMapDocument;
import org.mule.tools.devkit.ctf.junit.AbstractTestCase;

public class InsertDocumentTestCases extends AbstractTestCase<CouchbaseConnector> {

	public InsertDocumentTestCases() {
		super(CouchbaseConnector.class);
	}

	@Before
	public void setup() {
		// TODO
	}

	@After
	public void tearDown() {
		// TODO
	}

	@Test
	public void verify() {
		org.mule.modules.couchbase.model.CbMapDocument expected = null;
		org.mule.api.MuleEvent muleEvent = null;
		org.mule.modules.couchbase.model.CbMapDocument cbMapDocument = new CbMapDocument();
		assertEquals(getConnector().insertDocument(muleEvent, cbMapDocument), expected);
	}

}