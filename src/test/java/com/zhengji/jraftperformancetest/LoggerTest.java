package com.zhengji.jraftperformancetest;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/*
* @program jraft-performance-test
* @decription
* @author jianwei.wjw
* @create 2019-06-08 17:13:29
*/
public class LoggerTest {
	private Logger logger = LoggerFactory.getLogger(LoggerTest.class);

	@Test
	public void testLog() throws InterruptedException {
		logger.info("test");
		TimeUnit.SECONDS.sleep(10);
	}

}
