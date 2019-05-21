package com.demo.test.hello;


import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

    @RequestMapping("/hello")
    public String index(){
        System.out.println("hello spring");
        return "hello spring";
    }
}
