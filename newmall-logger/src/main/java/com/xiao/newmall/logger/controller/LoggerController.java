package com.xiao.newmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName LoggerController
 * @Description TODO
 * @Author WangYiXiao
 * @Date 2020/11/09 21:40
 * @Version 1.0
 **/
//@RestController  =  @Controller  +  @ResponseBody
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    //@ResponseBody返回值是网页 还是 文本
    //@Slf4j - lombok插件会帮你补充log的声明 和 实现 的代码
    @RequestMapping("/applog")
    public String applog(@RequestBody String logString){
        System.out.println(logString);
        log.info(logString);

        JSONObject jsonObject = JSON.parseObject(logString);
        if(jsonObject.getString("start")!=null){
            //启动日志
            kafkaTemplate.send("NEWMALL_STARTUP",logString);
        }else{
            //事件日志
            kafkaTemplate.send("NEWMAll_EVENT",logString);
        }





        return logString;
    }

}
