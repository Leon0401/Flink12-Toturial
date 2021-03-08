package com.stark.leon.demos.Beans;

public class WaterSensor {
	String id;
	Long ts;
	int vc;

	public WaterSensor() {
	}

	public WaterSensor(String id, Long ts, int vc) {
		this.id = id;
		this.ts = ts;
		this.vc = vc;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getTs() {
		return ts;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	public int getVc() {
		return vc;
	}

	public void setVc(int vc) {
		this.vc = vc;
	}

	@Override
	public String toString() {
		return "WaterSensor{" +
				"id='" + id + '\'' +
				", ts=" + ts +
				", vc=" + vc +
				'}';
	}
}
