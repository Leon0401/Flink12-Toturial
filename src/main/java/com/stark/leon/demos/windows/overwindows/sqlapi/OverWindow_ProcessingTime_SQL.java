package com.stark.leon.demos.windows.overwindows.sqlapi;

import com.stark.leon.demos.Beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @title: OverWindow_ProcessingTime_SQL
 * @author: Leon Stark
 * @date: 2021/5/9 11:22
 * @desc:
 * @msg:
 * @ref: https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/queries.html#group-windows
 */
public class OverWindow_ProcessingTime_SQL {
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

		// tableEnv.createTemporaryFunction("UTCToLocal", new UTCToLocal());

		// 4. 使用sql api 实现 基于 处理时间 的 会话窗口
		// 注意： 多个over window必须是同一类型的，要么都有界，要么都无界 ，并且有界时，界限必须一致,
		// 既然窗口是一致的，那么就可以简写为sql1中的形式。
		String sql = "" +
				"select " +
				"    id," +
				"    sum(vc) over(partition by id order by pt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as sum_vc," +
				"    count(id) over(partition by id order by pt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as cnt " +
				"from " + table +
				"";

		String sql1 = "" +
				"select " +
				"    id," +
				"    sum(vc) over w as sum_vc," +
				"    count(id) over w as cnt " +
				"from " + table +
				" window w as (partition by id order by pt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";

		String sql2 = "" +
				"select " +
				"    id," +
				"    sum(vc) over(partition by id order by pt) as sum_vc," +
				"    count(id) over(partition by id order by pt) as cnt " +
				"from " + table +
				"";
		Table result = tableEnv.sqlQuery(sql);

		// 5. 将结果表转换为流进行输出
		// tableEnv.toAppendStream(result, Row.class).print();

		// 5. 直接输出结果表
		result.execute().print();

		// 6. 执行
		env.execute();
	}
}
