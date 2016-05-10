package org.iidp.ostmap.rest_service;

import org.apache.log4j.Logger;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.io.*;

@SpringBootApplication
public class MainController {
    private static Logger log = Logger.getLogger(MainController.class);
    public static String configFilePath;

    /**
     * Main method to run the spring boot application.
     * @param args PAth to config file
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if(args != null && args.length > 0 && args[0] != ""){
            System.out.println("Use config file: " + args[0]);
            configFilePath = args[0];
        }else {
            System.out.println("Path to accumulo config file is necessary as parameter.");
        }
        SpringApplication.run(MainController.class, args);
    }

    /**
     * Global enable CORS for all mappings /api/**
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
     * @return String a json array of tweets as string
     */
    public static String getTestTweets()
    {
        String result = "";

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();

        try(BufferedReader br = new BufferedReader(new InputStreamReader(classloader.getResourceAsStream("example-response.json")))) {
            for(String line; (line = br.readLine()) != null; ) {
                result += line;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }


}
