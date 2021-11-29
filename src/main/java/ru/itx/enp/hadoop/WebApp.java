package ru.itx.enp.hadoop;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class WebApp {

	private String connectToHadoop() throws IOException {
		System.setProperty("java.security.krb5.realm", "ARENA.RU");
		System.setProperty("java.security.krb5.kdc", "master-0.arena.ru");
		Configuration conf = new Configuration();
		conf.addResource(new Path("conf/core-site.xml"));
		conf.addResource(new Path("conf/hdfs-site.xml"));
		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("hadoop.security.authorization", "true");
		conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		conf.set("dfs.client.use.datanode.hostname", "true");
		conf.set("dfs.namenode.kerberos.principal.pattern", "hdfs-namenode/*@ARENA.RU");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab("team0@ARENA.RU", "conf/team0.keytab");
		FileSystem fs = FileSystem.get(conf);
		return fs.getUri().toString();
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
