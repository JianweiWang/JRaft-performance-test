package com.zhengji.jraftperformancetest;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.net.InetAddress;

@SpringBootApplication public class JraftPerformanceTestApplication {

	public static void main(String[] args) throws IOException {
		SpringApplication.run(JraftPerformanceTestApplication.class, args);
		if (args.length >= 1) {
			PeerId peerId = new PeerId();
			InetAddress addr = InetAddress.getLocalHost();
			String ip = addr.getHostAddress().toString();
			if (!peerId.parse(ip + ":" + "8888")) {
				throw new IllegalArgumentException(
						"fail to parse node configuration: " + ip + ":8888");
			}
			;
			NodeOptions nodeOptions = new NodeOptions();
			nodeOptions.setElectionTimeoutMs(1000);
			nodeOptions.setSnapshotIntervalSecs(60);
			Configuration configuration = new Configuration();

			if (!configuration.parse(args[0])) {
				throw new IllegalArgumentException(
						"fail to parse peers configuration: " + args[0]);
			}

			nodeOptions.setInitialConf(configuration);

			DemoServer demoServer = new DemoServer(System.getProperty("user.home"),
					"jraft-demo", peerId, nodeOptions);

			System.out.println(
					"Started demo server at port: " + demoServer.getNode().getNodeId()
							.getPeerId().getPort());
		}
	}

}
