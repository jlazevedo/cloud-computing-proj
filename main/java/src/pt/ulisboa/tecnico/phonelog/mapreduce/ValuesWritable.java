package pt.ulisboa.tecnico.phonelog.mapreduce;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


/**
 * @author Diogo Antunes
 * @author Joao Azevedo
 * @version 2.0
 * 
 * The Class ValuesWritable.
 */
public class ValuesWritable implements WritableComparable<ValuesWritable> {

	/** The day. */
	private int day;
	
	/** The month. */
	private int month;
	
	/** The year. */
	private int year;
	
	/** The hours. */
	private int hours;
	
	/** The minutes. */
	private int minutes;
	
	/** The event id. */
	private int eventID;
	
	/** The data. */
	private Text data;
	
	/**
	 * Instantiates a new values writable.
	 */
	public ValuesWritable() {}
	
	/**
	 * Instantiates a new values writable.
	 *
	 * @param day the day
	 * @param month the month
	 * @param year the year
	 * @param hours the hours
	 * @param minutes the minutes
	 * @param eventID the event id
	 * @param data the data
	 */
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
	
	/**
	 * Instantiates a new values writable.
	 *
	 * @param dateTime the date time
	 * @param eventID the event id
	 * @param data the data
	 */
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
	
	/**
	 * Instantiates a new values writable.
	 *
	 * @param date the date
	 * @param time the time
	 * @param eventID the event id
	 * @param data the data
	 */
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

	/**
	 * Gets the day.
	 *
	 * @return the day
	 */
	public int getDay() {
		return day;
	}

	/**
	 * Sets the day.
	 *
	 * @param day the new day
	 */
	public void setDay(int day) {
		this.day = day;
	}

	/**
	 * Gets the month.
	 *
	 * @return the month
	 */
	public int getMonth() {
		return month;
	}

	/**
	 * Sets the month.
	 *
	 * @param month the new month
	 */
	public void setMonth(int month) {
		this.month = month;
	}

	/**
	 * Gets the year.
	 *
	 * @return the year
	 */
	public int getYear() {
		return year;
	}

	/**
	 * Sets the year.
	 *
	 * @param year the new year
	 */
	public void setYear(int year) {
		this.year = year;
	}

	/**
	 * Gets the hours.
	 *
	 * @return the hours
	 */
	public int getHours() {
		return hours;
	}

	/**
	 * Sets the hours.
	 *
	 * @param hours the new hours
	 */
	public void setHours(int hours) {
		this.hours = hours;
	}

	/**
	 * Gets the minutes.
	 *
	 * @return the minutes
	 */
	public int getMinutes() {
		return minutes;
	}

	/**
	 * Sets the minutes.
	 *
	 * @param minutes the new minutes
	 */
	public void setMinutes(int minutes) {
		this.minutes = minutes;
	}

	/**
	 * Gets the event id.
	 *
	 * @return the event id
	 */
	public int getEventID() {
		return eventID;
	}

	/**
	 * Sets the event id.
	 *
	 * @param eventID the new event id
	 */
	public void setEventID(int eventID) {
		this.eventID = eventID;
	}

	/**
	 * Gets the data.
	 *
	 * @return the data
	 */
	public String getData() {
		return data.toString();
	}

	/**
	 * Sets the data.
	 *
	 * @param data the new data
	 */
	public void setData(String data) {
		this.data = new Text(data);
	}
	
	/**
	 * Gets the date.
	 *
	 * @return the date
	 */
	public String getDate() {
		return  getDay()+"-"+getMonth()+"-"+getYear();
	}
	
	/**
	 * Date time.
	 *
	 * @return the string
	 */
	public String dateTime() {
		
		return getDay()+"-"+getMonth()+"-"+getYear()+"|"+getHours()+":"+getMinutes();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
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

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
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

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
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
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		
		return getDay()+"-"+getMonth()+"-"+getYear()+","+getHours()+":"+getMinutes()+
				","+getEventID()+","+getData();
	}

}
