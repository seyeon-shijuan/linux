package dataexpo;

import org.apache.hadoop.io.Text;

public class Airline {
	private int year;
	private int month;
	private int arriveDelayTime;
	private int departureDelayTime;
	private int distance;
	private boolean arriveDelayAvailable = true;
	private boolean departureDelayAvailable = true;
	private boolean distanceAvailable = true;
	private String uniqueCarrier;
	public Airline(Text text) {
		try {
			//csv 파일 : ,로 데이터 구분
			String[] columns = text.toString().split(",");
			year = Integer.parseInt(columns[0]); //연도를 숫자로
			month = Integer.parseInt(columns[1]); //월을 숫자로
			uniqueCarrier = columns[8]; //항공사 코드
			if(!columns[15].equals("NA")) {
				//NA : 출발지연 없는 경우
				departureDelayTime = Integer.parseInt(columns[15]);
			} else {
				departureDelayAvailable = false;	
			}
			if(!columns[14].equals("NA")) {
				//NA : 도착지연 없는 경우
				arriveDelayTime = Integer.parseInt(columns[14]);
			} else {
				arriveDelayAvailable = false;
			}
			if(!columns[18].equals("NA")) {
				//NA : 운항거리 없는 경우
				distance = Integer.parseInt(columns[18]);
			} else {
				distanceAvailable = false;
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}// Airline(Text text)
	
	public int getYear() {
		return year;
	}
	public int getMonth() {
		return month;
	}
	public int getArriveDelayTime() {
		return arriveDelayTime;
	}
	public int getDepartureDelayTime() {
		return departureDelayTime;
	}
	public int getDistance() {
		return distance;
	}
	public boolean isArriveDelayAvailable() {
		return arriveDelayAvailable;
	}
	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}
	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}
	public String getUniqueCarrier() {
		return uniqueCarrier;
	}
	
	
}
