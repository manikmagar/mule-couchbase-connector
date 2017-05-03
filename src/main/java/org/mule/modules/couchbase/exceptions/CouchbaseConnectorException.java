package org.mule.modules.couchbase.exceptions;

import org.mule.api.MessagingException;
import org.mule.api.MuleEvent;

public class CouchbaseConnectorException extends MessagingException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public CouchbaseConnectorException(MuleEvent event, Throwable cause) {
		super(event, cause);
	}

}
