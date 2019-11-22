package edu.northwestern.ssa;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import org.json.JSONObject;

import java.io.IOException;


public class parser implements RequestHandler<SQSEvent, Void>{

    public static void main(String[] args) {
        System.out.println("🐯 Hello again!");
    }

    @Override
    public Void handleRequest(SQSEvent sqsEvent, Context context) {

        String host = System.getenv("ELASTIC_SEARCH_HOST");
        String index = System.getenv("ELASTIC_SEARCH_INDEX");


        ElasticSearch es = new ElasticSearch("es");
        try {
            es.createIndex(host, index);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(SQSMessage msg : sqsEvent.getRecords()){
//            System.out.println(new String(msg.getBody()));

            String body = msg.getBody();

//            JSONObject bodyJSON = gson.toJson(body);
            JSONObject jsonObject = new JSONObject(body);

            try {
                es.postDocument(host, index, jsonObject);
            } catch (IOException e){
                e.printStackTrace();
            }

        }
        return null;
    }


//    public static void parser() {
//        String queueName = "queue";
//        SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build();
//        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
//                .queueUrl("https://sqs.us-east-1.amazonaws.com/757242523837/cwf8559_sqs")
//                .maxNumberOfMessages(5)
//                .build();
//        List<Message> messages= sqsClient.receiveMessage(receiveMessageRequest).messages();
//    }


}
