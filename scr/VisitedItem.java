
public abstract class VisitedItem {

	int orderNumber;

	boolean finished;

	String name;

	String joinDateTime;

	String leaveDateTime;

	public VisitedItem() {}

	public VisitedItem(int orderNumber, String name, String joinDateTime,
			String leaveDateTime) {
		this.orderNumber = orderNumber;
		this.name = name;
		this.joinDateTime = joinDateTime;
		this.leaveDateTime = leaveDateTime;
	}

	public int orderNumber() {
		return orderNumber;
	}

	public void setOrderNumber(int orderNumber) {
		this.orderNumber = orderNumber;
	}

	public boolean isFinished() {
		return finished;
	}

	public void setFinished(boolean finished) {
		this.finished = finished;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String getNormalizedName() {
		return name.split("-")[0];
	}

	public String getJoinDateTime() {
		return joinDateTime;
	}

	public void setJoinDateTime(String joinDateTime) {
		this.joinDateTime = joinDateTime;
	}

	public String getLeaveDateTime() {
		return leaveDateTime;
	}

	public void setLeaveDateTime(String leaveDateTime) {
		
		
		
		this.leaveDateTime = leaveDateTime;
	}

}
