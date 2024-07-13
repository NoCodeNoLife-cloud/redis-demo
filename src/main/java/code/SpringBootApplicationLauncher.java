package code;

// Copyright (c) 2024, NoCodeNoLife-cloud. All rights reserved.
// Author: NoCodeNoLife-cloud
// stay hungry，stay foolish
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.List;

// @EnableEurekaServer
// @EnableDiscoveryClient
// @EnableFeignClients
// @EnableDubbo
@SpringBootApplication
public class SpringBootApplicationLauncher {
	/**
	 * Entry point of the application.
	 *
	 * @param args The command line arguments.
	 */
	@SneakyThrows
	public static void main(String[] args) {
		SpringApplication.run(SpringBootApplicationLauncher.class, args);
	}

	/**
	 * This method creates and returns an ApplicationRunner bean.
	 * The ApplicationRunner bean is used to execute code when the application starts.
	 *
	 * @return an ApplicationRunner bean
	 */
	@Bean
	public ApplicationRunner applicationRunner(@Autowired RedissonService redissonService) {
		return args -> {
			List<String> list = new ArrayList<>();
			// list.add("word1");
			// list.add("word2");
			// list.add("word3");
			redissonService.addRListValue("hello", list);
			System.out.println(redissonService.getRListValue("hello"));
		};
	}
}