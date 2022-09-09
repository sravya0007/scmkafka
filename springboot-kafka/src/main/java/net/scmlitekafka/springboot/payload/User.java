package net.scmlitekafka.springboot.payload;

public class User {
	
	
	private int id;
	private String deviceId;
	private String sensorTemp;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getDeviceId() {
		return deviceId;
	}
	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}
	@Override
	public String toString() {
		return "User [id=" + id + ", deviceId=" + deviceId + ", sensorTemp=" + sensorTemp + "]";
	}
	public String getSensorTemp() {
		return sensorTemp;
	}
	public void setSensorTemp(String sensorTemp) {
		this.sensorTemp = sensorTemp;
	}

}
