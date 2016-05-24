package org.iidp.ostmap.rest_service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

@SpringBootApplication
public class MainController {
    static Logger log = LoggerFactory.getLogger(MainController.class);
    public static String configFilePath;

    /**
     * Main method to run the spring boot application.
     *
     * @param args PAth to config file
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args != null && args.length > 0 && !args[0].equals("")) {
            log.info("Use config file: " + args[0]);
            System.out.println("Use config file: " + args[0]);
            configFilePath = args[0];
        } else {
            System.out.println("Path to accumulo config file is necessary as parameter.");
        }
        SpringApplication.run(MainController.class, args);
    }

    /**
     * Global enable CORS for all mappings /api/**
     *
     * @return
     */
    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurerAdapter() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/api/**");
            }
        };
    }

    /**
     * A temporary method providing tweets. Can be deleted if the connection to accumulo was established.
     *
     * @return String a json array of tweets as string
     */
    public static String getTestTweets() {
        String result = "";

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(classloader.getResourceAsStream("example-response.json")))) {
            for (String line; (line = br.readLine()) != null; ) {
                result += line;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        /**
         * Artificial response delay to simulate accumulo backend worst-case
         */
        try {
            Thread.sleep(1000);                 //1000 milliseconds is one second.
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        return result;
    }


}
