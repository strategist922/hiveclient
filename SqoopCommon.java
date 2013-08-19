package com.gw.hiveclient;

import java.util.ArrayList;
import org.apache.hadoop.conf.*;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.ExportTool;

public class SqoopCommon {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://hfxcentos:9571");
		// conf.set("hadoop.job.ugi", "hadooper,hadoopgroup");
		conf.set("mapred.job.tracker", "hdfs://hfxcentos:9572");

		ArrayList<String> list = new ArrayList<String>();

		list.add("--table");
		list.add("logininfo"); // mysql中的表。将来数据要导入到这个表中
		list.add("--export-dir");
		list.add("/user/hive/warehouse/login"); // hdfs上的目录。这个目录下的数据要导入到mysql。
		list.add("--connect");
		list.add("jdbc:mysql://hfxcentos:3306/test"); // mysql的链接
		list.add("--username");
		list.add("root");
		list.add("--password");
		list.add("111111");

		list.add("--lines-terminated-by");
		list.add("\\n");// 数据的换行符号
		list.add("-m");
		list.add("1");// 定义mapreduce的数量

		String[] arg = new String[1];

		ExportTool exporter = new ExportTool();
		Sqoop sqoop = new Sqoop(exporter);

		sqoop.setConf(conf);

		arg = list.toArray(new String[0]);

		int result = Sqoop.runSqoop(sqoop, arg);

		System.out.println("res:" + result);
	}

}
