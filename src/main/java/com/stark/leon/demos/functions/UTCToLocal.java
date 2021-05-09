package com.stark.leon.demos.functions;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * @title: UTCToLocal
 * @author: Leon Stark
 * @date: 2021/5/9 16:34
 * @desc:
 * @msg:
 */
public class UTCToLocal extends ScalarFunction {
	public Timestamp eval(Timestamp s) {
		long timestamp = s.getTime() + 28800000;
		return new Timestamp(timestamp);
	}
}