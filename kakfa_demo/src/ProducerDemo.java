import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by zaish on 2017-7-6.
 */
public class ProducerDemo {
    public static void main(String[] args) {
        //指定jaas文件
        System.setProperty("java.security.auth.login.config","E:\\demo\\idea_projects\\kakfademo\\src\\main\\resources\\jaas.conf");
        //是否异步发送
        boolean isAsync=true;
        Properties properties=new Properties();
        //producer配置
        properties.put("bootstrap.servers","transwarp-1:9092");
        properties.put("client.id","DemoProducer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //安全认证配置
        properties.put("security.protocol","SASL_PLAINTEXT");
        properties.put("sasl.mechanism","GSSAPI");
        properties.put("sasl.kerberos.service.name","kafka");
        KafkaProducer producer=new KafkaProducer(properties);
        String topic="demo1";
        int messageNo=1;
        while (true){
            String messageInfo="Message: "+messageNo;
            long startTime=System.currentTimeMillis();
            if(isAsync){//异步发送
                producer.send(new ProducerRecord(topic,messageNo,messageInfo),new DemoCallback(startTime,messageNo,messageInfo));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{//同步发送
                try {
                    producer.send(new ProducerRecord(topic,messageNo,messageInfo)).get();
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }finally {
                }
            }
            ++messageNo;
        }
    }
    static class DemoCallback implements Callback{
        private final long startTime;
        private final int key;
        private final String message;
        public DemoCallback(long startTime, int key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            long elapsedTime=System.currentTimeMillis()-startTime;
            if(recordMetadata!=null){
                System.out.println("message("+key+","+message+") sent to partition("
                        +recordMetadata.partition()+"), offset("+recordMetadata.offset()+") in "+elapsedTime);
            }else{
                e.printStackTrace();
            }
        }
    }
}
