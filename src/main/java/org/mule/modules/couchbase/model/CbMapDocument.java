package org.mule.modules.couchbase.model;

import java.util.Map;

import org.mule.api.annotations.Required;
import org.mule.api.annotations.display.FriendlyName;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * This is wrapper pojo over {@link JsonDocument}. Document content is represented as Java {@link Map}.
 * @author manik
 *
 */
public class CbMapDocument {
	/**
	 * Unique document identifier
	 */
	@Required
	private String id;
	
	/**
	 * Document CAS id used for concurrent locking
	 */
	@FriendlyName("Document CAS")
	private long cas;
	
	/**
	 * Document expiration time in seconds
	 */
	@FriendlyName("Document Expiration Time")
	private int expiry;
	
	/**
	 * {@link Map} representing the document content.
	 */
	@FriendlyName("Document Content")
	@Required
	private Map<String, Object> content;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public long getCas() {
		return cas;
	}
	public void setCas(long cas) {
		this.cas = cas;
	}
	public int getExpiry() {
		return expiry;
	}
	public void setExpiry(int expiry) {
		this.expiry = expiry;
	}
	public Map<String, Object> getContent() {
		return content;
	}
	public void setContent(Map<String, Object> content) {
		this.content = content;
	}
	
	/**
	 * Creates an instance of {@link CbMapDocument} from {@link JsonDocument}.
	 * @param jsonDocument
	 * @return {@link CbMapDocument}
	 */
	public static CbMapDocument fromJsonDocument(JsonDocument jsonDocument){
		CbMapDocument cbMapDocument = new CbMapDocument();
		cbMapDocument.setCas(jsonDocument.cas());
		cbMapDocument.setExpiry(jsonDocument.expiry());
		cbMapDocument.setId(jsonDocument.id());
		cbMapDocument.setContent(jsonDocument.content().toMap());
		return cbMapDocument;
	}
	
	/**
	 * Creates {@link JsonDocument} from this {@link CbMapDocument}. 
	 * @return {@link JsonDocument}
	 */
	public JsonDocument toJsonDocument(){
		JsonObject jsonObject = JsonObject.from(this.getContent());
		
		JsonDocument document = JsonDocument.create(id, jsonObject);
		
		return document;
		
	}
	
	@Override
	public String toString() {
		return "Id: " + this.id + ", CAS: "+ this.cas + ", Expiry: " + this.expiry + ", Content: " + this.content.toString();
	}
	
}
