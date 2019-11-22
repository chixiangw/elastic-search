package edu.northwestern.ssa;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class downloader_copy {

    public static void main(String[] args) throws IOException {
        System.out.println("🐯hello!");
        requestWARC();
    }

    public static void requestWARC() throws IOException {

        String newestFileName = getNewestFileName();

        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        GetObjectRequest s3Request = GetObjectRequest.builder()
                .bucket("commoncrawl")
                .key(newestFileName)  // the newestFileName
//                .key("crawl-data/CC-NEWS/2019/09/CC-NEWS-20190901022141-01066.warc.gz")
                .build();

        InputStream is = s3.getObject(s3Request, ResponseTransformer.toInputStream());  //from STarzia

        // create an object of type ArchiveReader
        ArchiveReader archivereader = WARCReaderFactory.get(newestFileName, is, true);                   //from STarzia


        SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build();
        String queueName = "queue";

        CreateQueueRequest createQueueRequest = CreateQueueRequest
                .builder()
                .queueName(queueName)
                .build();
        sqsClient.createQueue(createQueueRequest);


        //iterate through the ArchiveRecords provided by the ArchiveReader
        // each ArchiveRecords is an HTTP response.
        int i = 0;
//        ExecutorService pool = Executors.newFixedThreadPool(80);
        for (ArchiveRecord r : archivereader) {
//            System.out.println("<split>--------------------------------------------------------------------------------------------------");
//            System.out.println("🐯1. Header: " + r.getHeader());
//            System.out.println("🐯2. URL: " + getTheUrl(r));
//            System.out.println("🐯3. Title: " + getBodyDoc(r).title());
//            System.out.println("🐯4. Text: " + getBodyDoc(r).text());

            JSONObject jsonobj = new JSONObject();

            jsonobj.put("title", getBodyDoc(r).title());
            jsonobj.put("txt", getBodyDoc(r).text());
            jsonobj.put("url", getTheUrl(r));

            i++;
            if (i%10 == 0){
                System.out.println("\nSend message - " + i);
            }


            // snippet-start:[sqs.java2.sqs_example.send_message]
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl("https://sqs.us-east-1.amazonaws.com/757242523837/cwf8559_sqs")
//                    .messageBody("Hello world!")
                    .messageBody(String.valueOf(jsonobj))
                    .delaySeconds(10)
                    .build());
        }
    }


    //example function from AWS tutorial
    public String myHandler(int myCount, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("received : " + myCount);
        return String.valueOf(myCount);
    }

    private static String getTheUrl(ArchiveRecord ar) {
        return ar.getHeader().getUrl();
    }

    private static Document getBodyDoc(ArchiveRecord ar) throws IOException {
        byte[] rawFullRecord = new byte[ar.available()];

        for (int i = 0;
             i < rawFullRecord.length;
             i += ar.read(rawFullRecord, i, rawFullRecord.length - i)) {
        }

        String data = new String(rawFullRecord);

        String recordBody;
        if (data.contains("\r\n\r\n")){
            recordBody = data.substring(data.indexOf("\r\n\r\n") + 4);
        } else {
            recordBody = data;
        }
//        String recordBody = data.substring(data.indexOf("\r\n\r\n") + 4);
        Document doc = Jsoup.parse(recordBody);

//        System.out.println("🐯raw data: "+data);
//        System.out.println("🐯Length of raw data: " + data.length());
//        System.out.println("🐯 Length of body:" + recordBody.length());
//        System.out.println("🐶Body: "+ recordBody);
        return doc;
    }

    private static String getNewestFileName(){
        String bucketName = "commoncrawl";

        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
//                .overrideConfiguration(ClientOverrideConfiguration.builder().apiCallTimeout(Duration.ofMinutes(30)).build())
                .build();

        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request
                .builder()
                .bucket(bucketName)
                .prefix("crawl-data/CC-NEWS/2019/11")
                .build();

        ListObjectsV2Response listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
        List<S3Object> objectListing = listObjectsV2Response.contents();

        return objectListing.get(objectListing.size()-1).key();
    }
}