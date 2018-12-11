package edu.uchicago.leimao.kafka_simulated_crime;

public class CrimeRecord {
	public CrimeRecord(String caseNumber, String community, int year, int month, int day) {
		super();
		this.caseNumber = caseNumber;
		this.community = community;
		this.year = year;
		this.month = month;
		this.day = day;
		//this.date = String.format("%04d", this.year) + String.format("%02d", this.month) + String.format("%02d", this.day);
	}
	public String getCaseNumber() {
		return caseNumber;
	}
	/*
	public String getDate() {
		return date;
	}*/
	public String getCommunity() {
		return community;
	}
	public int getYear() {
		return year;
	}
	public int getMonth() {
		return month;
	}
	public int getDay() {
		return day;
	}
	
	String caseNumber;
	//String date;
	String community;
	int year;
	int month;
	int day;
	
}
