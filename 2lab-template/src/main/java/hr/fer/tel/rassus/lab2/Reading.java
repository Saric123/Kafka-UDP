package hr.fer.tel.rassus.lab2;

import java.util.Arrays;
import java.util.List;

public class Reading {
	
	private String myMeasure;
	private int scalarTimeMark;
	private int[] vectorTimeMark = new int[3];
	
	public Reading(String myMeasure, int scalarTimeMark, int[] list) {
		this.myMeasure = myMeasure;
		this.scalarTimeMark = scalarTimeMark;
		this.vectorTimeMark = list;
	}
	
	public void setVectorTimeMark(int[] vectorTimeMark) {
		this.vectorTimeMark = vectorTimeMark;
	}
	
	public String getMeasure() {
		return myMeasure;
	}
	
	public int getScalarTimeMark() {
		return scalarTimeMark;
	}
	public int[] getVectorTimeMark() {
		return vectorTimeMark;
	}
	
	public String toString() {
		return "( " + myMeasure + ", " + scalarTimeMark + ", " + Arrays.toString(vectorTimeMark) + " )";
	}
	
	
}
