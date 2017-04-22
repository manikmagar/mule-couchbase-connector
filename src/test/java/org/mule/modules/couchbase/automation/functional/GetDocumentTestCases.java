package org.mule.modules.couchbase.automation.functional;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.couchbase.CouchbaseConnector;
import org.mule.tools.devkit.ctf.junit.AbstractTestCase;

public class GetDocumentTestCases extends AbstractTestCase<CouchbaseConnector> {

	public GetDocumentTestCases() {
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
		java.util.Map<java.lang.String, java.lang.Object> expected = null;
		org.mule.api.MuleEvent muleEvent = null;
		java.lang.String id = null;
		boolean refreshExpirationTime;
		int refreshTime;
		//assertEquals(getConnector().getDocument(muleEvent, id, refreshExpirationTime, refreshTime), expected);
	}

}