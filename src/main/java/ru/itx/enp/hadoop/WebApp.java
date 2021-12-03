package ru.itx.enp.hadoop;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class WebApp {

	private String connectToHadoop() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("conf/core-site.xml"));
		conf.addResource(new Path("conf/hdfs-site.xml"));
		conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		System.setProperty("HADOOP_USER_NAME", "team20");
		try (FileSystem fs = FileSystem.get(conf)) {
			try (FSDataInputStream is = fs.open(new Path("/user/team20/fake.txt"))) {
				StringWriter writer = new StringWriter();
				IOUtils.copy(is, writer, "UTF-8");
				System.out.print(writer.toString());
			}
		}
		return "OK";
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
