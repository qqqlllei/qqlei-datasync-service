package com.qqlei.shop.datasync.rabbitmq;

import com.qqlei.shop.datasync.service.ProductService;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import redis.clients.jedis.JedisCluster;

import com.alibaba.fastjson.JSONObject;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * 数据同步服务，就是获取各种原子数据的变更消息,这种方式的接口通知是时效性比较低一点的接口调用
 * 
 * （1）然后通过spring cloud fegion调用product-service服务的各种接口， 获取数据
 * （2）将原子数据在redis中进行增删改
 * （3）将维度数据变化消息写入rabbitmq中另外一个queue，供数据聚合服务来消费
 *
 * 
 * @author 李雷
 *
 */
@Component  
@RabbitListener(queues = "data-change-queue")  
public class DataChangeQueueReceiver {  
	
	@Autowired
	private ProductService eshopProductService;

	@Autowired
	private JedisCluster jedisCluster;
	@Autowired
	private RabbitMQSender rabbitMQSender;

	private Set<String> dimDataChangeMessageSet = Collections.synchronizedSet(new HashSet<>());

	public DataChangeQueueReceiver() {
		new Thread(new SendThread()).start();
	}
  
    @RabbitHandler  
    public void process(String message) {

		System.out.println("================================同步服务收到消息【"+message+"】========================================");
    	// 对这个message进行解析
    	JSONObject jsonObject = JSONObject.parseObject(message);
    	
    	// 先获取data_type
    	String dataType = jsonObject.getString("data_type");  
    	if("brand".equals(dataType)) {
    		processBrandDataChangeMessage(jsonObject);  
    	} else if("category".equals(dataType)) {
    		processCategoryDataChangeMessage(jsonObject); 
    	} else if("product_intro".equals(dataType)) {
    		processProductIntroDataChangeMessage(jsonObject); 
    	} else if("product_property".equals(dataType)) {
    		processProductPropertyDataChangeMessage(jsonObject);
     	} else if("product".equals(dataType)) {
     		processProductDataChangeMessage(jsonObject); 
     	} else if("product_specification".equals(dataType)) {
     		processProductSpecificationDataChangeMessage(jsonObject);  
     	}
    }  
    
    private void processBrandDataChangeMessage(JSONObject messageJSONObject) {
    	Long id = messageJSONObject.getLong("id"); 
    	String eventType = messageJSONObject.getString("event_type"); 
    	
    	if("add".equals(eventType) || "update".equals(eventType)) { 
    		JSONObject dataJSONObject = JSONObject.parseObject(eshopProductService.findBrandById(id));
			jedisCluster.set("brand_" + dataJSONObject.getLong("id"), dataJSONObject.toJSONString());
    	} else if ("delete".equals(eventType)) {
			jedisCluster.del("brand_" + id);
    	}
		dimDataChangeMessageSet.add("{\"dim_type\": \"brand\", \"id\": " + id + "}");

		System.out.println("【品牌维度数据变更消息被放入内存Set中】,brandId=" + id);
    }
    
    private void processCategoryDataChangeMessage(JSONObject messageJSONObject) {
    	Long id = messageJSONObject.getLong("id"); 
    	String eventType = messageJSONObject.getString("event_type"); 
    	
    	if("add".equals(eventType) || "update".equals(eventType)) { 
    		JSONObject dataJSONObject = JSONObject.parseObject(eshopProductService.findCategoryById(id));
			jedisCluster.set("category_" + dataJSONObject.getLong("id"), dataJSONObject.toJSONString());
    	} else if ("delete".equals(eventType)) {
			jedisCluster.del("category_" + id);
    	}

		dimDataChangeMessageSet.add("{\"dim_type\": \"category\", \"id\": " + id + "}");
    }
    
    private void processProductIntroDataChangeMessage(JSONObject messageJSONObject) {
    	Long id = messageJSONObject.getLong("id"); 
    	Long productId = messageJSONObject.getLong("product_id");
    	String eventType = messageJSONObject.getString("event_type"); 
    	
    	if("add".equals(eventType) || "update".equals(eventType)) { 
    		JSONObject dataJSONObject = JSONObject.parseObject(eshopProductService.findProductIntroById(id));
			jedisCluster.set("product_intro_" + productId, dataJSONObject.toJSONString());
    	} else if ("delete".equals(eventType)) {
			jedisCluster.del("product_intro_" + productId);
    	}
		dimDataChangeMessageSet.add("{\"dim_type\": \"product\", \"id\": " + productId + "}");
    }
    
    private void processProductDataChangeMessage(JSONObject messageJSONObject) {
    	Long id = messageJSONObject.getLong("id"); 
    	String eventType = messageJSONObject.getString("event_type"); 
    	
    	if("add".equals(eventType) || "update".equals(eventType)) { 
    		JSONObject dataJSONObject = JSONObject.parseObject(eshopProductService.findProductById(id));
			jedisCluster.set("product_" + id, dataJSONObject.toJSONString());
    	} else if ("delete".equals(eventType)) {
			jedisCluster.del("product_" + id);
    	}
		dimDataChangeMessageSet.add("{\"dim_type\": \"product\", \"id\": " + id + "}");
    }
    
    private void processProductPropertyDataChangeMessage(JSONObject messageJSONObject) {
    	Long id = messageJSONObject.getLong("id"); 
    	Long productId = messageJSONObject.getLong("product_id");
    	String eventType = messageJSONObject.getString("event_type"); 
    	
    	if("add".equals(eventType) || "update".equals(eventType)) { 
    		JSONObject dataJSONObject = JSONObject.parseObject(eshopProductService.findProductPropertyById(id));
			jedisCluster.set("product_property_" + productId, dataJSONObject.toJSONString());
    	} else if ("delete".equals(eventType)) {
			jedisCluster.del("product_property_" + productId);
    	}
		dimDataChangeMessageSet.add("{\"dim_type\": \"product\", \"id\": " + productId + "}");
    }
    
    private void processProductSpecificationDataChangeMessage(JSONObject messageJSONObject) {
    	Long id = messageJSONObject.getLong("id"); 
    	Long productId = messageJSONObject.getLong("product_id");
    	String eventType = messageJSONObject.getString("event_type"); 
    	
    	if("add".equals(eventType) || "update".equals(eventType)) { 
    		JSONObject dataJSONObject = JSONObject.parseObject(eshopProductService.findProductSpecificationById(id));
			jedisCluster.set("product_specification_" + productId, dataJSONObject.toJSONString());
    	} else if ("delete".equals(eventType)) {
			jedisCluster.del("product_specification_" + productId);
    	}
		dimDataChangeMessageSet.add("{\"dim_type\": \"product\", \"id\": " + productId + "}");
    }

    private class SendThread implements Runnable{

		@Override
		public void run() {
			while (true){
				if(!dimDataChangeMessageSet.isEmpty()) {
					for(String message : dimDataChangeMessageSet) {
						rabbitMQSender.send("aggr-data-change-queue", message);
						System.out.println("【将去重后的维度数据变更消息发送到下一个queue】,message=" + message);
					}
					dimDataChangeMessageSet.clear();
				}
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
  
}  