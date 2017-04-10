/**
 * 
 */
package gov.pnnl.aristotle.aiminterface;

/**
 * @author puro755
 *
 */
public class NousSearchQuestionStreamRecord {

	public NousSearchQuestionStreamRecord() {
		super();
		// TODO Auto-generated constructor stub
	}

	private int version;
	private String uuid;
	private long timestemp;
	private String source;
	private String destination;
	private int maxpathsize;
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
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getDestination() {
		return destination;
	}
	public void setDestination(String destination) {
		this.destination = destination;
	}
	public int getMaxpathsize() {
		return maxpathsize;
	}
	public void setMaxpathsize(int maxpathsize) {
		this.maxpathsize = maxpathsize;
	}

	
}
