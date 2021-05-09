package com.stark.leon.demos.windows.groupwindows.sql;

import com.stark.leon.demos.Beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.rowInterval;

/**
 * @title: TumblingWindow_ProcessingTime_SQL
 * @author: Leon Stark
 * @date: 2021/5/9 11:22
 * @desc:
 * @msg:
 * @ref: https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/queries.html#group-windows
 */
public class TumblingWindow_ProcessingTime_SQL {
	public static void main(String[] args) throws Exception {
		// 1. 获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 2. 读取端口数据创建刘并转换为Java Bean
		DataStream<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
				.map(data -> {
					String[] split = data.split(",");
					return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
				});

		// 3. 将流转换为表并指定处理时间
		Table table = tableEnv.fromDataStream(waterSensorDS,
				$("id"),
				$("ts"),
				$("vc"),
				$("pt").proctime()
		);

		// 4. 使用sql api 实现 基于 处理时间 的 滚动窗口
		Table result = tableEnv.sqlQuery("select " +
				"id," +
				"TUMBLE_START(pt,INTERVAL '5' second) as wStart," +
				" count(id) as cnt, from " + table +
				" group by id,tumble(pt,INTERVAL '5' second)");

		// 5. 将结果表转换为流进行输出
		tableEnv.toAppendStream(result, Row.class).print();

		// 6. 执行
		env.execute();
	}
}
