package org.mule.modules.couchbase.model;

import java.util.HashMap;
import java.util.Map;

import org.mule.api.annotations.Required;
import org.mule.api.annotations.display.FriendlyName;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.param.Default;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * This is wrapper pojo over {@link JsonDocument}. Document content is represented as Java {@link Map}.
 * @author manik
 *
 */
public class CbMapDocument implements Cloneable {
	/**
	 * Unique document identifier
	 */
	@Required
	@FriendlyName("Unique ID")
	@Placement(group="Document Metadata",order=1)
	private String id;
	
	/**
	 * Document CAS id used for concurrent locking. For server to consider this CAS, value must be >= 0.
	 */
	@FriendlyName("CAS")
	@Placement(group="Document Metadata",order=2)
	@Default("0")
	private long cas;
	
	/**
	 * Document expiration time in seconds. For server to consider expiry, value must be >= 0.
	 */
	@FriendlyName("Expiration Time")
	@Placement(group="Document Metadata",order=3)
	@Default("-1")
	private int expiry;
	
	/**
	 * {@link Map} representing the document content.
	 */
	@FriendlyName("Content Map")
	@Required
	@Placement(group="Document Content",order=1)
	private Map<String, Object> content;
	
	
	public CbMapDocument(){
		
	}
	
	public CbMapDocument(String id){
		this(id, null);
	}
	
	public CbMapDocument(String id, Map<String, Object> content){
		setId(id);
		setContent(content);
	}
	
	
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
		if(jsonDocument.content() != null) {
			cbMapDocument.setContent(jsonDocument.content().toMap());
		}
		
		return cbMapDocument;
	}
	
	/**
	 * Creates {@link JsonDocument} from this {@link CbMapDocument}. 
	 * @return {@link JsonDocument}
	 */
	public JsonDocument toJsonDocument(){
		JsonObject jsonObject = JsonObject.from(this.getContent());
		
		JsonDocument document = null;
		
		if (expiry >= 0) {
			document = JsonDocument.create(this.getId(), this.getExpiry(), jsonObject, this.getCas());
		} else {
			document = JsonDocument.create(getId(), jsonObject, getCas());
		}
		
		return document;
		
	}
	
	@Override
	public String toString() {
		return "Id: " + this.id + ", CAS: "+ this.cas + ", Expiry: " + this.expiry + ", Content: " + this.content.toString();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public CbMapDocument clone() {
		CbMapDocument newDoc = new CbMapDocument();
		newDoc.setId(this.getId());
		newDoc.setCas(this.getCas());
		newDoc.setExpiry(getExpiry());
		newDoc.setContent((Map<String, Object>) ((HashMap<String, Object>)getContent()).clone());
		return newDoc;
	}

	/**
	 * Checks if Content is null or empty.
	 * @return
	 */
	public boolean hasContent(){
		return content == null || content.isEmpty();
	}
	
}
