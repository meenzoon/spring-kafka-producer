package meen.zoon.kafka.producer.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class Producer {

  /*
   * http body 값을 이용 하여 json 값을 받은 다음 그 값을 kafka 로 Produce 하는 클래스
   */
  @ResponseBody
  @PostMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public String callProducer(@RequestBody String jsonParameter) {
    //Gson gson = new Gson();

    //JsonObject object = new JsonParser().parse(jsonParameter).getAsJsonObject();

    Properties properties = new Properties();

    // kafka server 설정
    properties.put("bootstrap.servers", "");
    // key, value serialize, deserialize 할 class 설정
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties
        .put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    properties.put("group.id", "");
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("auto.offset.reset", "earliest");
    properties.put("session.timeout.ms", "30000");
    properties.put("security.protocol", "SASL_SSL");
    properties.put("sasl.mechanism", "SCRAM-SHA-256");

    properties.put("sasl.jaas.config",
        "org.apache.kafka.common.security.scram.ScramLoginModule required username= password=;");

    KafkaProducer<String, String> producer = new KafkaProducer<>(
        properties);

    try {
      producer.send(new ProducerRecord<>("", jsonParameter));
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      producer.flush();
      producer.close();
    }

    //String jsonString = gson.toJson(object);
    return jsonParameter;
  }
}
