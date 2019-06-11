package com.zhengji.jraftperformancetest.controller;

/*
* @program jraft-performance-test
* @decription
* @author jianwei.wjw
* @create 2019-06-08 16:51:54
*/

import com.alipay.remoting.exception.RemotingException;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import com.taobao.vipserver.sdk.HttpClient;
import com.zhengji.jraftperformancetest.rpc.AddIPRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

@Controller
public class DemoClientController {
	Logger logger = LoggerFactory.getLogger(DemoClientController.class);
	@RequestMapping("/jraft-test")
	@ResponseBody
	public Long test(@RequestParam("groupId") String groupId,
			@RequestParam("groupConfStr") String confStr,
			@RequestParam("testCount") int testCount,
			@RequestParam("threadCount") int nThread,
			@RequestParam("dom") String dom,
			@RequestParam("serverIP") String serverIp)
			throws TimeoutException, InterruptedException, RemotingException {
		executorService = Executors.newFixedThreadPool(nThread);
		return call(groupId, confStr, testCount, dom, serverIp);
	}

	private ExecutorService executorService = Executors.newFixedThreadPool(16);

	private long call(String groupId, String confStr, int testCount, String dom, String serverIp)
			throws TimeoutException, InterruptedException {
		final Configuration conf = new Configuration();
		if (!conf.parse(confStr)) {
			throw new IllegalArgumentException("Fail to parse conf:" + confStr);
		}

		RouteTable.getInstance().updateConfiguration(groupId, conf);

		final BoltCliClientService cliClientService = new BoltCliClientService();
		cliClientService.init(new CliOptions());

		if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
			throw new IllegalStateException("Refresh leader failed");
		}

		final PeerId leader = RouteTable.getInstance().selectLeader(groupId);
		System.out.println("leader: " + leader.toString());
		final CountDownLatch latch = new CountDownLatch(testCount);
		final long start = System.currentTimeMillis();
		Random random = new Random(System.currentTimeMillis());
		for (int i = 0; i < testCount; i++) {
			String ip = "192.168." + random.nextInt(256) +  "." + random.nextInt(256) +
					":80_1_DEFAULT";
			try {
				executorService.submit(() -> {
					try {
						HttpClient.HttpResult result = HttpClient.httpGet("http://" + serverIp + "/vipserver/api/addIP4Dom0?dom="
								+ dom + "&ipList=" + ip, null, null, "utf-8");
//						addIP(cliClientService, leader, ip, latch);
						if (result.code != 200) {
							System.out.println(result.content);
						}
					} catch (Throwable throwable) {
						throwable.printStackTrace();
					}
					finally {
						latch.countDown();
					}
				});

			} catch (Throwable throwable) {
				logger.error("failed to add ip", throwable);
			}
		}

		latch.await();

		return System.currentTimeMillis() - start;
	}

	private  void addIP(final BoltCliClientService cliClientService, final PeerId leader,
			final String  ip, CountDownLatch latch) throws
			RemotingException,
			InterruptedException {
		final AddIPRequest request = new AddIPRequest();
		request.setIp(ip);
		cliClientService.getRpcClient().invokeSync(leader.getEndpoint().toString(), request, 1000);
	}

	public static void main(String[] args) throws TimeoutException, InterruptedException {
		DemoClientController clientController = new DemoClientController();
//		System.out.println(clientController.call("jraft-demo",
//				"11.160.67.108:8888,11.163.169.205:8888,11.239.112.36:8888",
//				100000));
	}
}
