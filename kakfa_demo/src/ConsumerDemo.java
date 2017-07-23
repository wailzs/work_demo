import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by zaish on 2017-7-6.
 */
public class ConsumerDemo {
    public static void main(String[] args) {
        //指定jaas文件
        System.setProperty("java.security.auth.login.config","E:\\demo\\idea_projects\\kakfademo\\src\\main\\resources\\jaas.conf");
        Properties properties=new Properties();
        //consumer配置
        properties.put("bootstrap.servers","transwarp-1:9092");
        properties.put("group.id","test");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("va" +
                "lue.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //安全认证配置
        properties.put("security.protocol","SASL_PLAINTEXT");
        properties.put("sasl.mechanism","GSSAPI");
        properties.put("sasl.kerberos.service.name","kafka");
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        //订阅topic
        consumer.subscribe(Arrays.asList("demo1"));
        try {
            while (true){
                //每次poll可拉取多个消息
                ConsumerRecords<String,String> records=consumer.poll(200);
                for(ConsumerRecord<String,String> record : records){
                    System.out.printf("offset=%d,key=%s,value=%s\n",record.offset(),record.key(),record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
