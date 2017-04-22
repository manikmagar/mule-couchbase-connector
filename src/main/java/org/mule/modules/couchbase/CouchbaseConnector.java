package org.mule.modules.couchbase;

import java.util.Map;

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
import org.mule.util.StringUtils;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

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
    					@Default("15") int refreshTime){
   		
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
		
		CbMapDocument doc = CbMapDocument.fromJsonDocument(document);
		
		return doc;
    	
    }
   	
   	/**
   	 * This method update/inserts the JSON document for given id into couchbase database.
   	 * @param muleEvent
   	 * @param id Unique id for document
   	 * @param contentMap {@link Map<String, Object>} expression evaluating to Document content.
   	 * @return {@link CbMapDocument} containing document id, cas, expiry and content from database.
   	 * @see Bucket#upsert(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="UpSert A Document")
   	public CbMapDocument upsertDocument(MuleEvent muleEvent, String id, @FriendlyName("Document Contents Map") Map<String, Object> contentMap){
   		
   		Bucket bucket = openBucket();
		
		JsonObject jsonObject = JsonObject.from(contentMap);
		
		JsonDocument document = JsonDocument.create(id, jsonObject);
		
		document = bucket.upsert(document);
		
		CbMapDocument cbMapDocument = CbMapDocument.fromJsonDocument(document);
   		
   		return cbMapDocument;
   	}
   	
   	/**
   	 * This method tries to insert the JSON document for given id into couchbase database. It will throw an exception if document with given id already exists in database.
   	 * @param muleEvent
   	 * @param id Unique id for document
   	 * @param contentMap {@link Map<String, Object>} expression evaluating to Document content.
   	 * @return {@link CbMapDocument} containing document id, cas, expiry and content from database.
   	 * @see Bucket#insert(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="Insert A Document")
   	public CbMapDocument insertDocument(MuleEvent muleEvent, String id, @FriendlyName("Document Contents Map") Map<String, Object> contentMap){
   		Bucket bucket = openBucket();
		
		JsonObject jsonObject = JsonObject.from(contentMap);
		
		JsonDocument document = JsonDocument.create(id, jsonObject);
		
		document = bucket.insert(document);
		
		CbMapDocument cbMapDocument = CbMapDocument.fromJsonDocument(document);
   		
   		return cbMapDocument;
   		
   	}
   	
   	/**
   	 * This method tries to update an existing JSON document for given id into couchbase database. It will throw an exception if document with given id Does not exist.
   	 * 
   	 * @param muleEvent
   	 * @param id Unique id for document
   	 * @param contentMap {@link Map<String, Object>} expression evaluating to Document content.
   	 * @return {@link CbMapDocument} containing document id, cas, expiry and content from database.
   	 * @see Bucket#replace(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="Update A Document")
   	public CbMapDocument updateDocument(MuleEvent muleEvent, String id, @FriendlyName("Document Contents Map") Map<String, Object> contentMap){
   		Bucket bucket = openBucket();
		
		JsonObject jsonObject = JsonObject.from(contentMap);
		
		JsonDocument document = JsonDocument.create(id, jsonObject);
		
		document = bucket.replace(document);
		
		CbMapDocument cbMapDocument = CbMapDocument.fromJsonDocument(document);
   		
   		return cbMapDocument;
   		
   	}
   	
   	/**
   	 * This method removes the document with given id from database. Returned {@link CbMapDocument} only contains ID and CAS value set as document is already removed from server.
   	 * 
   	 * @param muleEvent
   	 * @param id Unique id for document
   	 * @param contentMap {@link Map<String, Object>} expression evaluating to Document content.
   	 * @return {@link CbMapDocument} containing document id, cas, expiry and content from database.
   	 * @see Bucket#remove(com.couchbase.client.java.document.Document)
   	 */
   	
   	@Processor(friendlyName="Remove A Document")
   	public CbMapDocument removeDocument(MuleEvent muleEvent, String id, @FriendlyName("Document Contents Map") Map<String, Object> contentMap){
   		Bucket bucket = openBucket();
		
		JsonObject jsonObject = JsonObject.from(contentMap);
		
		JsonDocument document = JsonDocument.create(id, jsonObject);
		
		document = bucket.remove(document);
		
		CbMapDocument cbMapDocument = CbMapDocument.fromJsonDocument(document);
   		
   		return cbMapDocument;
   		
   	}
   	
   	/**
   	 * This method unlocks the document that was previously locked (pessimistic). Document can be unlocked by providing either document ID and CAS OR whole document. Document can be retrieved with get document method. 
   	 * 
   	 * @param muleEvent
   	 * @param id Unique id for document
   	 * @param cas current CAS value from server
   	 * @param cbMapDocument {@link CbMapDocument} representing current document with id, cas and content.
   	 * @return {@link Boolean} result if document unlocked successfully
   	 * @see Bucket#remove(com.couchbase.client.java.document.Document)
   	 */
   	
   	@Processor(friendlyName="Unlock A Document")
   	public boolean unlockDocument(MuleEvent muleEvent, 
   			@Placement(group="By Id") @FriendlyName("Id") String id, 
   			@Placement(group="By Id") @FriendlyName("CAS") long cas, 
   			@Placement(group="By Document") @FriendlyName("Document Contents Map") CbMapDocument cbMapDocument){
   		Bucket bucket = openBucket();
		boolean unlocked = false;
   		if(StringUtils.isNotBlank(id)){
   			unlocked = bucket.unlock(id, cas);
   		} else {
   			JsonObject jsonObject = JsonObject.from(cbMapDocument.getContent());
   			
   			JsonDocument document = JsonDocument.create(cbMapDocument.getId(), jsonObject);
   			
   			unlocked = bucket.unlock(document);
   			
   		}
   		return unlocked;
   		
   	}


}