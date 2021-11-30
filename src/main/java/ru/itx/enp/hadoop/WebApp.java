package ru.itx.enp.hadoop;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class WebApp {

	private static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	private class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	private String connectToHadoop() throws IOException {
		Configuration conf = new Configuration();
		System.setProperty("java.security.krb5.realm", "ARENA.RU");
		System.setProperty("java.security.krb5.kdc", "master-0.arena.ru");
		conf.addResource(new Path("conf/core-site.xml"));
		conf.addResource(new Path("conf/yarn-site.xml"));
		conf.addResource(new Path("conf/hdfs-site.xml"));
		conf.addResource(new Path("conf/mared-site.xml"));
		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("hadoop.security.authorization", "true");
		conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("dfs.client.use.datanode.hostname", "true");
		conf.set("dfs.namenode.kerberos.principal.pattern", "hdfs-namenode/*@ARENA.RU");
		conf.set("yarn.resourcemanager.principal.pattern", "yarn-resourcemanager/*@ARENA.RU");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab("team35@ARENA.RU", "conf/team35.keytab");
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WebApp.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.getConfiguration().set("mapreduce.job.queuename", "team35");
		FileInputFormat.addInputPath(job, new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output" + (System.currentTimeMillis() % 10000)));
		try {
			job.waitForCompletion(true);
			return "OK : "+job.toString();
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			return "FAIL : "+e.toString();
		}
		
	}

	public WebApp() throws IOException {
		HttpServer server = HttpServer.create();
		server.bind(new InetSocketAddress(8080), 0);
		server.createContext("/", new HttpHandler() {
			public void handle(HttpExchange exchange) throws IOException {
				String response = connectToHadoop();
				System.out.println(response);
				exchange.sendResponseHeaders(200, response.length());
				try (OutputStream os = exchange.getResponseBody()) {
					os.write(response.getBytes());
				}
			}
		});
		server.start();
	}

	public static void main(String[] args) throws Exception {
		new WebApp();
	}

}
