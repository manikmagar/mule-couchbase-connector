package org.mule.modules.couchbase.automation.functional;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.couchbase.CouchbaseConnector;
import org.mule.tools.devkit.ctf.junit.AbstractTestCase;

public class UpsertDocumentTestCases extends AbstractTestCase<CouchbaseConnector> {

	public UpsertDocumentTestCases() {
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
//		org.mule.modules.couchbase.model.CbMapDocument expected = null;
//		org.mule.api.MuleEvent muleEvent = null;
//		java.lang.String id = null;
//		java.util.Map<java.lang.String, ?> contentMap = null;
//		assertEquals(getConnector().upsertDocument(muleEvent, id, contentMap), expected);
	}

}