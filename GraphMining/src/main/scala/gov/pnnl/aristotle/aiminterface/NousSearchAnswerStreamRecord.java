/**
 * @author puro755
 *
 * 
 */
package gov.pnnl.aristotle.aiminterface;

/**
 * @author puro755
 *
 */
public class NousSearchAnswerStreamRecord {
	private int version;
	private String uuid;
	private long timestemp;
	private String[] value;
	private String source;
	private String destination;
	
	
	public String getDestination() {
		return destination;
	}
	public void setDestination(String destination) {
		this.destination = destination;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}
	public String getUuid() {
		return uuid;
	}
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
	public long getTimestemp() {
		return timestemp;
	}
	public void setTimestemp(long timestemp) {
		this.timestemp = timestemp;
	}
	public String[] getValue() {
		return value;
	}
	public void setValue(String[] value) {
		this.value = value;
	}
	
	

}
