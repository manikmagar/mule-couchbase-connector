package org.mule.modules.couchbase.automation.functional;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.couchbase.CouchbaseConnector;
import org.mule.tools.devkit.ctf.junit.AbstractTestCase;

public class UnlockDocumentTestCases extends AbstractTestCase<CouchbaseConnector> {

	public UnlockDocumentTestCases() {
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
		boolean expected = true;
		org.mule.api.MuleEvent muleEvent = null;
		org.mule.modules.couchbase.model.CbMapDocument cbMapDocument = null;
		assertEquals(getConnector().unlockDocument(muleEvent, cbMapDocument), expected);
	}

}