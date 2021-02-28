package com.example.springbootApi;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import java.net.InetAddress;
import java.net.UnknownHostException;

// hbase的gson和springboot引入的冲突
@SpringBootApplication(exclude = {GsonAutoConfiguration.class})
@Slf4j
public class SpringBootApiApplication {

    public static void main(String[] args) throws UnknownHostException {
        ConfigurableApplicationContext application = SpringApplication.run(SpringBootApiApplication.class, args);
        Environment env = application.getEnvironment();
        String applicationName = env.getProperty("server.servlet.context-path").substring(1);
        log.info("\n----------------------------------------------------------\n\t" +
                        "Application '{}' is running! Access URLs:\n\t" +
                        "External: \thttp://{}:{}/{}\n\t"+
                        "Doc: \thttp://{}:{}/{}/doc.html\n"+
                        "----------------------------------------------------------",
                applicationName,
                InetAddress.getLocalHost().getHostAddress(),
                env.getProperty("server.port"),
                applicationName,
                InetAddress.getLocalHost().getHostAddress(),
                env.getProperty("server.port"),
                applicationName);
    }

}
