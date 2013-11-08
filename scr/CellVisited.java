

public class CellVisited extends VisitedItem {

	public CellVisited() {}

	public CellVisited(int orderNumber, String name, String joinDateTime,
			String leaveDateTime) {
		this.orderNumber = orderNumber;
		this.name = name + "-cell";
		this.joinDateTime = joinDateTime;
		this.leaveDateTime = leaveDateTime;
	}

}