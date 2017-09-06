package java;

import cn.com.jetflow.yita.Flow;
import cn.com.jetflow.yita.InstanceConfiguration;
import cn.com.jetflow.yita.Workflow;
import cn.com.jetflow.yita.YITA;
import cn.com.jetflow.yita.bin.BufferedAggregator;
import cn.com.jetflow.yita.conf.Configuration;
import cn.com.jetflow.yita.resource.file.FileResourceReader;
import cn.com.jetflow.yita.sim.SimulationConfig;
import cn.com.jetflow.yita.transform.Transform;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
public class YitaKafkaExample {
        public static class MySimConf extends SimulationConfig {
        public MySimConf(String path) {
            super();
            if (this.configuration == null) this.configuration = new Configuration();
            this.configuration.add("license.file", path);
        }
    }
        public static void main(String[] args) throws Exception {
                if (args.length < 1) {
                        System.out.println("yitaExample topic-name");
                        return;
                }
                try {
                        //InstanceConfiguration conf = new MySimConf("./conf/license.xml");
                        YITA.initialize();
                        final Workflow workflow = new Workflow();
			//workflow.setDefaultAggregator(new BufferedAggregator(1000000,1000));
                        Map<String, Integer> topicToPartitionCount = new HashMap<>();
                        topicToPartitionCount.put(args[0], 24);
                        java.DirectKafkaReader reader = new java.DirectKafkaReader(topicToPartitionCount);
                    reader.setName("file-reader");
			            reader.setPartitionCount(24);
                        final Transform<byte[], byte[], String, String> displayLine = new Transform<byte[], byte[], String, String>() {
                                @Override
                                public void apply(byte[] offset, byte[] value, Flow context) throws IOException {
                                        //String line = new String(value, StandardCharsets.UTF_8);
                                       // try {
                                               // JSONObject obj = new JSONObject(line);
                                                //String id = obj.getString("ID");
                                                //String time = obj.getString("TIME0");
                                                long cur_time = System.currentTimeMillis();
                                                System.out.print(cur_time + ", " + "1" + "\n");
                                        //} catch (JSONException e) {
                                         //       e.printStackTrace();
                                        //}
                               }
                        };
                        displayLine.setName("print");
                        workflow.add(reader, displayLine);
                        reader.bindPush(displayLine).synchronous();
                        workflow.execute();
                } finally {
                        if (YITA.isInitialized()) {
                                YITA.shutdown();
                        }
                }
        }
}
