package pt.ulisboa.tecnico.phonelog.mapreduce;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class ValuesWritable implements WritableComparable<ValuesWritable> {

	private int day;
	private int month;
	private int year;
	private int hours;
	private int minutes;
	
	private int eventID;
	
	private Text data;
	
	public ValuesWritable() {}
	
	public ValuesWritable(int day, int month, int year, int hours, int minutes,
			int eventID, String data) {
		
		setDay(day);
		setMonth(month);
		setYear(year);
		setHours(hours);
		setMinutes(minutes);
		setEventID(eventID);
		setData(data);
	}
	
	public ValuesWritable(String dateTime, int eventID, String data) {
		
		String[] pieces1 = dateTime.split(",");
		
		String[] date = pieces1[0].split("-");
		String[] time = pieces1[1].split(":");
		
		setDay(Integer.parseInt(date[0]));
		setMonth(Integer.parseInt(date[1]));
		setYear(Integer.parseInt(date[2]));
		setHours(Integer.parseInt(time[0]));
		setMinutes(Integer.parseInt(time[1]));
		setEventID(eventID);
		setData(data);
	}
	
	public ValuesWritable(String date, String time, int eventID, String data) {
		
		String[] dateAux = date.split("-");
		String[] timeAux = time.split(":");
		
		setDay(Integer.parseInt(dateAux[0]));
		setMonth(Integer.parseInt(dateAux[1]));
		setYear(Integer.parseInt(dateAux[2]));
		setHours(Integer.parseInt(timeAux[0]));
		setMinutes(Integer.parseInt(timeAux[1]));
		setEventID(eventID);
		setData(data);
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getHours() {
		return hours;
	}

	public void setHours(int hours) {
		this.hours = hours;
	}

	public int getMinutes() {
		return minutes;
	}

	public void setMinutes(int minutes) {
		this.minutes = minutes;
	}

	public int getEventID() {
		return eventID;
	}

	public void setEventID(int eventID) {
		this.eventID = eventID;
	}

	public String getData() {
		return data.toString();
	}

	public void setData(String data) {
		this.data = new Text(data);
	}
	
	public String getDate() {
		return  getDay()+"-"+getMonth()+"-"+getYear();
	}
	
	public String dateTime() {
		
		return getDay()+"-"+getMonth()+"-"+getYear()+"|"+getHours()+":"+getMinutes();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		setDay(in.readInt());
		setMonth(in.readInt());
		setYear(in.readInt());
		setHours(in.readInt());
		setMinutes(in.readInt());
		setEventID(in.readInt());
		setData(in.readUTF());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(getDay());
		out.writeInt(getMonth());
		out.writeInt(getYear());
		out.writeInt(getHours());
		out.writeInt(getMinutes());
		out.writeInt(getEventID());
		out.writeUTF(getData());
	}

	@Override
	public int compareTo(ValuesWritable other) {
		
		if (getYear()<other.getYear())
			return -1;
		else 
			if (getYear()>other.getYear())
				return 1;
			else
				if (getMonth()<other.getMonth())
					return -1;
				else
					if (getMonth()>other.getMonth())
						return 1;
					else
						if (getDay()<other.getDay())
							return -1;
						else
							if (getDay()>other.getDay())
								return 1;
							else
								if (getHours()<other.getHours())
									return -1;
								else
									if (getHours()>other.getHours())
										return 1;
									else
										if (getMinutes()<other.getMinutes())
											return -1;
										else
											if (getMinutes()>other.getMinutes())
												return 1;
											else
												if (getEventID()<other.getEventID())
												return -1;
												else
													if (getEventID()>other.getEventID())
													return 1;
													else return getData().compareTo(other.getData());
							
	}
	
	@Override
	public String toString() {
		
		return getDay()+"-"+getMonth()+"-"+getYear()+","+getHours()+":"+getMinutes()+
				","+getEventID()+","+getData();
	}

}
