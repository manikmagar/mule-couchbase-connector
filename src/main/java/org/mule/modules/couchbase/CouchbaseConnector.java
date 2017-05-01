package org.mule.modules.couchbase;

import javax.inject.Inject;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import org.mule.api.MuleContext;
import org.mule.api.MuleEvent;
import org.mule.api.annotations.Config;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.display.FriendlyName;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.display.Summary;
import org.mule.api.annotations.param.Default;
import org.mule.modules.couchbase.config.CouchbaseConnectorConfig;
import org.mule.modules.couchbase.model.CbMapDocument;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;

@Connector(name="couchbasedb", friendlyName="Couchbase DB")
public class CouchbaseConnector{

    @Config
    CouchbaseConnectorConfig config;
    
    @Inject
    private MuleContext muleContext;
    
	public MuleContext getMuleContext() {
		return muleContext;
	}

	public void setMuleContext(MuleContext muleContext) {
		this.muleContext = muleContext;
	}

	public CouchbaseConnectorConfig getConfig() {
        return config;
    }

    public void setConfig(CouchbaseConnectorConfig config) {
        this.config = config;
    }
    
    public Bucket openBucket(){
    	return config.openBucket();
    }
    
    /**
     * This method retrieves document with given id from couchbase database. It can optionally lock and/or refresh the expiration time of document.
     * @param muleEvent
     * @param id Unique identifier of document
     * @param lockDocument
     * @param lockDuration
     * @param refreshExpirationTime
     * @param refreshTime
     * @return {@link CbMapDocument} containing document id, cas, expiry and content from database.
     * @see {@link Bucket#get(com.couchbase.client.java.document.Document)}, {@link Bucket#getAndLock(com.couchbase.client.java.document.Document, int)}, {@link Bucket#getAndTouch(com.couchbase.client.java.document.Document)}
     */
    @Processor(friendlyName="Get A Document")
    public CbMapDocument getDocument(MuleEvent muleEvent, String id, 
    		@Placement(group="Document Locking (pessimistic)") @Default("false") boolean lockDocument,
    		@Placement(group="Document Locking (pessimistic)") @Max(30) @Min(0) @Summary("Define seconds for which document should be write-locked. This creates pissimistic locking. Read more on couchbase documentation.") @Default("15") int lockDuration,
    		@Placement(group="Refresh Expiration Time") @Default("false") boolean refreshExpirationTime,
    		@Placement(group="Refresh Expiration Time") @Summary("Time in seconds. If you specify an expiration time greater than 30 days in seconds (60 seconds * 60 minutes * 24 hours * 30 days = 2,592,000 seconds) it is considered an absolute time stamp instead of a relative one") 
    					@Default("0") int refreshTime){
   		
   		Bucket bucket = openBucket();
		
		JsonDocument document = null;
		
		if(lockDocument){
			if(refreshExpirationTime) bucket.touch(id, refreshTime);
   			document = bucket.getAndLock(id, lockDuration);
   		} else if (refreshExpirationTime) {
   			document = bucket.getAndTouch(id, refreshTime);
   		} else {
   			document =  bucket.get(id);
   		}
		
		if(document == null){
			return null;
		} else {
			CbMapDocument doc = CbMapDocument.fromJsonDocument(document);
			return doc;
		}
		
    }
   	
   	/**
   	 * This method update/inserts the JSON document for given id into couchbase database.
   	 * @param muleEvent
   	 * @param cbMapDocument {@link CbMapDocument} containing document id and content to be upserted in database.
   	 * @return {@link CbMapDocument} containing document id, cas, expiry and content from database.
   	 * @see com.couchbase.client.java.Bucket#upsert(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="UpSert A Document")
   	public CbMapDocument upsertDocument(MuleEvent muleEvent, @Placement(group="Document to upsert") @FriendlyName("Document") @Summary("Specify the document to be upserted.") CbMapDocument cbMapDocument){
   		
   		Bucket bucket = openBucket();
		
		JsonDocument document = cbMapDocument.toJsonDocument();
		
		document = bucket.upsert(document);
		
		CbMapDocument returnDocument = CbMapDocument.fromJsonDocument(document);
   		
   		return returnDocument;
   	}
   	
   	/**
   	 * This method tries to insert the JSON document for given id into couchbase database. It will throw an exception if document with given id already exists in database.
   	 * @param muleEvent
   	 * @param cbMapDocument {@link CbMapDocument} containing document id and content to be inserted in database.
   	 * @return {@link CbMapDocument} containing document id, cas, expiry and content from database.
   	 * @see com.couchbase.client.java.Bucket#insert(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="Insert A Document")
   	public CbMapDocument insertDocument(MuleEvent muleEvent, @Placement(group="Document to Insert") @FriendlyName("Document") @Summary("Specify the document to be inserted.") CbMapDocument cbMapDocument){
   		
   		Bucket bucket = openBucket();
		
		JsonDocument document = cbMapDocument.toJsonDocument();
		
		document = bucket.insert(document);
		
		CbMapDocument returnDocument = CbMapDocument.fromJsonDocument(document);
   		
   		return returnDocument;
   		
   	}
   	
   	/**
   	 * This method tries to update an existing JSON document for given id into couchbase database. It will throw an exception if document with given id Does not exist.
   	 * 
   	 * @param muleEvent
   	 * @param cbMapDocument {@link CbMapDocument} containing document id and content to be updated in database.
   	 * @return {@link CbMapDocument} containing document id, cas, expiry and content from database.
   	 * @see com.couchbase.client.java.Bucket#replace(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="Update A Document")
   	public CbMapDocument updateDocument(MuleEvent muleEvent, @Placement(group="Document to Update") @FriendlyName("Document") @Summary("Specify the document to be updated.") CbMapDocument cbMapDocument){
   		
   		Bucket bucket = openBucket();
		
		JsonDocument document = cbMapDocument.toJsonDocument();
		
		document = bucket.replace(document);
		
		CbMapDocument returnDocument = CbMapDocument.fromJsonDocument(document);
   		
   		return returnDocument;
   		
   	}
   	
   	/**
   	 * This method removes the document with given id from database. Returned {@link CbMapDocument} only contains ID and CAS value set as document is already removed from server.
   	 * 
   	 * @param muleEvent
   	 * @param cbMapDocument {@link CbMapDocument} containing document id and content to be removed from database.
   	 * @return {@link CbMapDocument} containing document id, cas, expiry and content from database.
   	 * @see com.couchbase.client.java.Bucket#remove(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="Delete A Document")
   	public CbMapDocument deleteDocument(MuleEvent muleEvent, @Placement(group="Document to Delete") @FriendlyName("Document") @Summary("Specify the document to be deleted.") CbMapDocument cbMapDocument){
   		
   		Bucket bucket = openBucket();
		
		JsonDocument document = cbMapDocument.toJsonDocument();
		
		document = bucket.remove(document);
		
		CbMapDocument returnDocument = CbMapDocument.fromJsonDocument(document);
   		
   		return returnDocument;
   		
   	}
   	
   	/**
   	 * This method unlocks the document that was previously locked (pessimistic). Document can be unlocked by providing either document ID and CAS OR whole document. Document can be retrieved with get document method. 
   	 * 
   	 * @param muleEvent
   	 * @param cbMapDocument {@link CbMapDocument} containing document id and content to be unlocked from database.
   	 * @return {@link Boolean} if document is successfully unlocked
   	 * @see com.couchbase.client.java.Bucket#unlock(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="Unlock A Document")
   	public boolean unlockDocument(MuleEvent muleEvent, @Placement(group="Document to be Unlocked") @FriendlyName("Document") @Summary("Specify the document to be unlocked.") CbMapDocument cbMapDocument){
   		
   		Bucket bucket = openBucket();
		
		JsonDocument document = cbMapDocument.toJsonDocument();
		
		boolean unlocked = bucket.unlock(document);
		
   		
   		return unlocked;
   		
   	}


}