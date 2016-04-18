package com.watchdog.model;

import java.io.Serializable;
import java.util.Date;

import scala.util.parsing.combinator.testing.Str;

public class UsageTables implements Serializable{
Date datetoday;
float avg;
String tablename;
public UsageTables(Date datetoday, float avg, String tablename) {
	super();
	this.datetoday = datetoday;
	this.avg = avg;
	this.tablename = tablename;
}
public Date getDatetoday() {
	return datetoday;
}
public void setDatetoday(Date datetoday) {
	this.datetoday = datetoday;
}
public float getAvg() {
	return avg;
}
public void setAvg(float avg) {
	this.avg = avg;
}
public String getTablename() {
	return tablename;
}
public void setTablename(String tablename) {
	this.tablename = tablename;
}

}
