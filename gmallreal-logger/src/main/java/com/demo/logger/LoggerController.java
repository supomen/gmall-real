package com.demo.logger;


import org.springframework.web.bind.annotation.*;

@RestController
public class LoggerController {

    @PostMapping("/log")
    public String doLog(@RequestParam("log") String log){
        System.out.println(log);
        return "success";
    }

}
