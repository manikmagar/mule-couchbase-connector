package org.mule.modules.couchbase.helper;

import java.util.Map;

import org.mule.modules.couchbase.CouchbaseConnector;
import org.mule.modules.couchbase.model.CbMapDocument;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class DocumentsHelper {
	
	public static Map<String, Object> getDocument(final String id, CouchbaseConnector connector){
		Bucket bucket = connector.openBucket();
		
		JsonDocument document = null;
		document = bucket.get(id);
		
		Map<String, Object> doc = document.content().toMap();
		
		return doc;
	}
	
	public static Map<String, Object> getDocumentAndLock(final String id, CouchbaseConnector connector, int lockTime){
		Bucket bucket = connector.openBucket();
		
		JsonDocument document = null;
		
		document = bucket.getAndLock(id,lockTime);
		
		Map<String, Object> doc = document.content().toMap();
		
		return doc;
	}
	
	public static Map<String, Object> getDocumentAndTouch(final String id, CouchbaseConnector connector, int touchDuration){
		Bucket bucket = connector.openBucket();
		
		JsonDocument document = null;
		
		document = bucket.getAndTouch(id, touchDuration);
		
		Map<String, Object> doc = document.content().toMap();
		
		return doc;
	}
	
	
	public static CbMapDocument upsertDocument(final String id, final Map<String, Object> contentMap, CouchbaseConnector connector){
		Bucket bucket = connector.openBucket();
		
		JsonObject jsonObject = JsonObject.from(contentMap);
		
		JsonDocument document = JsonDocument.create(id, jsonObject);
		
		document = bucket.upsert(document);
		
		return CbMapDocument.fromJsonDocument(document);
		
	}
}
