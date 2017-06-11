/**
 * The software in this package is published under the terms of the Apache v2.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.couchbase;

import java.util.ArrayList;
import java.util.List;
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
import org.mule.extension.annotations.param.Optional;
import org.mule.modules.couchbase.config.CouchbaseConnectorConfig;
import org.mule.modules.couchbase.model.JavaMapDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.MessagingException;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

@Connector(name="couchbasedb", friendlyName="Couchbase DB",
			minMuleVersion="3.5.3",keywords="Couchbase, Database",
			description="Mule Connector for Couchbase NoSQL database")
public class CouchbaseConnector{
	
	private Logger LOG = LoggerFactory.getLogger(CouchbaseConnector.class); 

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
     * @return {@link JavaMapDocument} containing document id, cas, expiry and content from database.
     * @see Bucket#get(com.couchbase.client.java.document.Document)
     * @see Bucket#getAndLock(com.couchbase.client.java.document.Document, int)
     * @see Bucket#getAndTouch(com.couchbase.client.java.document.Document)
     */
    @Processor(friendlyName="Get Document")
    public JavaMapDocument getDocument(MuleEvent muleEvent, String id, 
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
			JavaMapDocument doc = JavaMapDocument.fromJsonDocument(document);
			return doc;
		}
		
    }
   	
   	/**
   	 * This method update/inserts the JSON document for given id into couchbase database.
   	 * @param muleEvent
   	 * @param javaMapDocument {@link JavaMapDocument} containing document id and content to be upserted in database.
   	 * @return {@link JavaMapDocument} containing document id, cas, expiry and content from database.
   	 * @see com.couchbase.client.java.Bucket#upsert(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="UpSert Document")
   	public JavaMapDocument upsertDocument(MuleEvent muleEvent, @Placement(group="Document to upsert") @FriendlyName("Document") @Summary("Specify the document to be upserted.") JavaMapDocument javaMapDocument){
   		
   		Bucket bucket = openBucket();
		
		JsonDocument document = javaMapDocument.toJsonDocument();
		
		document = bucket.upsert(document);
		
		JavaMapDocument returnDocument = JavaMapDocument.fromJsonDocument(document);
   		
   		return returnDocument;
   	}
   	
   	/**
   	 * This method tries to insert the JSON document for given id into couchbase database. It will throw an exception if document with given id already exists in database.
   	 * @param muleEvent
   	 * @param javaMapDocument {@link JavaMapDocument} containing document id and content to be inserted in database.
   	 * @return {@link JavaMapDocument} containing document id, cas, expiry and content from database.
   	 * @see com.couchbase.client.java.Bucket#insert(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="Insert Document")
   	public JavaMapDocument insertDocument(MuleEvent muleEvent, @Placement(group="Document to Insert") @FriendlyName("Document") @Summary("Specify the document to be inserted.") JavaMapDocument javaMapDocument){
   		
   		Bucket bucket = openBucket();
		
		JsonDocument document = javaMapDocument.toJsonDocument();
		
		document = bucket.insert(document);
		
		JavaMapDocument returnDocument = JavaMapDocument.fromJsonDocument(document);
   		
   		return returnDocument;
   		
   	}
   	
   	/**
   	 * This method tries to update an existing JSON document for given id into couchbase database. It will throw an exception if document with given id Does not exist.
   	 * 
   	 * @param muleEvent
   	 * @param javaMapDocument {@link JavaMapDocument} containing document id and content to be updated in database.
   	 * @return {@link JavaMapDocument} containing document id, cas, expiry and content from database.
   	 * @see com.couchbase.client.java.Bucket#replace(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="Update Document")
   	public JavaMapDocument updateDocument(MuleEvent muleEvent, @Placement(group="Document to Update") @FriendlyName("Document") @Summary("Specify the document to be updated.") JavaMapDocument javaMapDocument){
   		
   		Bucket bucket = openBucket();
		
		JsonDocument document = javaMapDocument.toJsonDocument();
		
		if(!bucket.exists(document.id())) {
			throw new DocumentDoesNotExistException("Document with id "+ document.id() +" does not exist.");
		}
		
		document = bucket.replace(document);
		
		JavaMapDocument returnDocument = JavaMapDocument.fromJsonDocument(document);
   		
   		return returnDocument;
   		
   	}
   	
   	/**
   	 * This method removes the document with given id from database. Returned {@link JavaMapDocument} only contains ID and CAS value set as document is already removed from server.
   	 * 
   	 * @param muleEvent
   	 * @param javaMapDocument {@link JavaMapDocument} containing id of the document to be removed from server. Content value is ignored.
   	 * @return {@link JavaMapDocument} containing document id, cas, expiry and content from database.
   	 * @see com.couchbase.client.java.Bucket#remove(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="Delete Document")
   	public boolean deleteDocument(MuleEvent muleEvent, @Placement(group="Document to Delete") @FriendlyName("Document") @Summary("Specify the document to be deleted.") JavaMapDocument javaMapDocument){
   		
   		Bucket bucket = openBucket();
		
   		if(!bucket.exists(javaMapDocument.getId())) {
			LOG.error("Document with id "+ javaMapDocument.getId() +" does not exist.", new DocumentDoesNotExistException("Document with id "+ javaMapDocument.getId() +" does not exist."));
			return false;
		}
   		
		JsonDocument document = bucket.remove(javaMapDocument.getId());
		
		try {
			JavaMapDocument.fromJsonDocument(document);
		} catch (DocumentDoesNotExistException notExists) {
			LOG.info("Couchbase Delete: Document to delete does not exist: "+ javaMapDocument.getId());
			return false;
		}catch (Exception otherEx) {
			LOG.info("Couchbase Delete: Unable to delete document: "+ javaMapDocument.getId());
			return false;
		}
		
   		
   		return true;
   		
   	}
   	
   	/**
   	 * This method unlocks the document that was previously locked (pessimistic). Document can be unlocked by providing either document ID and CAS OR whole document. Document can be retrieved with get document method. 
   	 * 
   	 * @param muleEvent
   	 * @param id Unique Id of the document
   	 * @param cas Long CAS value that must match with the one stored in database.
   	 * @return {@link Boolean} if document is successfully unlocked
   	 * @see com.couchbase.client.java.Bucket#unlock(com.couchbase.client.java.document.Document)
   	 */
   	@Processor(friendlyName="Unlock Document")
   	public boolean unlockDocument(MuleEvent muleEvent, @Placement(group="Document to be Unlocked") @FriendlyName("Document ID") @Summary("Specify the document Id to be unlocked.") String id,
   			@Placement(group="Document to be Unlocked") @FriendlyName("Document CAS") @Summary("Document CAS must match with the one stored in database.") Long cas){
   		
   		Bucket bucket = openBucket();
   		
   		boolean unlocked = bucket.unlock(id, cas);
		
   		return unlocked;
   		
   	}
   	
   	/**
   	 * Runs the N1QL query and result the result set. Optionally, either positional or named parameters can be provided.
   	 * @param muleEvent
   	 * @param query to be executed.
   	 * @param positionalParams {@link List} containing positional parameters to replace place holders in query. Takes precedence over named parameters.
   	 * @param namedParams {@link Map} containing named parameters to be replaced in query.
   	 * @return {@link List} containing the documents in map.
   	 */
   	@Processor(friendlyName="Execute Query")
   	public List<Map<String, Object>> executeQuery(MuleEvent muleEvent, String query, @Optional @FriendlyName("Positional Parameters") 
   				@Summary("For positional parameters, specify list of param values. For named parameters, specify Map of key-value pairs.") Object params){
   		
   		Bucket bucket = openBucket();
   		
   		bucket.bucketManager().createN1qlPrimaryIndex(true, false);
   		
   		N1qlQueryResult result;
   		
   		if(params != null && params instanceof List){
   			result = bucket.query(N1qlQuery.parameterized(query, JsonArray.from(params)));
   		} else if (params != null && params instanceof Map) {
   			result = bucket.query(N1qlQuery.parameterized(query, JsonObject.from((Map)params)));
   		} else {
   			result = bucket.query(N1qlQuery.simple(query));
   		}
   		
   		if(!result.parseSuccess()){
   			LOG.error("Failed to parse the query with errors: " + result.errors());
   			throw new MessagingException("Failed to parse the N1QL Query: "+ query);
   		}
   		
   		if(!result.finalSuccess()){
   			LOG.error("Failed to execute the query with errors: "+ result.errors());
   			throw new MessagingException("Failed to execute the N1QL Query: "+ query);
   		}
   		
   		List<Map<String, Object>> resultSet = new ArrayList<Map<String,Object>>();
   		
   		for (N1qlQueryRow row : result.allRows()){
   			resultSet.add(row.value().toMap());
   		}
   		
   		return resultSet;
   		
   	}


}