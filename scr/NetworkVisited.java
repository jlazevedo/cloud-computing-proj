import java.util.ArrayList;



public class NetworkVisited extends VisitedItem {

	public NetworkVisited() {}

	public NetworkVisited(int orderNumber, String name, String joinDateTime,
			String leaveDateTime) {
		this.orderNumber = orderNumber;
		this.name = name + "-network";
		this.joinDateTime = joinDateTime;
		this.leaveDateTime = leaveDateTime;
	}

}