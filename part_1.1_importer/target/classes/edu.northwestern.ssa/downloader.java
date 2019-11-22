package edu.northwestern.ssa;

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
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public class downloader {

    public static void main(String[] args) throws IOException {
        System.out.println("🐯 Hello!");
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
                .build();

        InputStream is = s3.getObject(s3Request, ResponseTransformer.toInputStream());  //from STarzia

        // create an object of type ArchiveReader
        ArchiveReader archivereader = WARCReaderFactory.get(newestFileName, is, true);     //from STarzia

        SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build();


        //iterate through the ArchiveRecords provided by the ArchiveReader
        // each ArchiveRecords is an HTTP response.
        int i = 0;
        for (ArchiveRecord r : archivereader) {

//            System.out.println("🐯1. Header: " + r.getHeader());
//            System.out.println("🐯2. URL: " + getTheUrl(r));
//            System.out.println("🐯3. Title: " + getBodyDoc(r).title());
//            System.out.println("🐯4. Text: " + getBodyDoc(r).text());

            JSONObject jsonobj = new JSONObject();

            getBodyDoc(r).ifPresent(doc -> {
                jsonobj.put("title", doc.title());
                jsonobj.put("txt", doc.text());
                jsonobj.put("url", getTheUrl(r));
                System.out.println("1.🐯   " + doc.title());
                System.out.println("2.🐯   " + getTheUrl(r));
                System.out.println("3.🐯   "+doc.text());
            });

//            jsonobj.put("title", getBodyDoc(r).title());
//            jsonobj.put("txt", getBodyDoc(r).text());
//            jsonobj.put("url", getTheUrl(r));

            i++;
            if (i%10 == 0){
                System.out.println("\nSend message - " + i);
//                System.out.println(getBodyDoc(r).text());
            }

            // snippet-start:[sqs.java2.sqs_example.send_message]
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl("https://sqs.us-east-1.amazonaws.com/757242523837/cwf8559_sqs")
                    .messageBody(String.valueOf(jsonobj))  //attention: sent the JsonString to SQS
//                    .messageBody(jsonobj.toString());    //or use this? which is good?
                    .delaySeconds(10)
                    .build());
        }
    }

    private static String getTheUrl(ArchiveRecord ar) {
        return ar.getHeader().getUrl();
    }

    private static Optional<Document> getBodyDoc(ArchiveRecord ar) throws IOException {
        byte[] rawFullRecord = new byte[ar.available()];

        for (int i = 0;
             i < rawFullRecord.length;
             i += ar.read(rawFullRecord, i, rawFullRecord.length - i)) {
        }

        String data = new String(rawFullRecord);
        String recordBody;
        Document doc;

        try {
            recordBody = data.substring(data.indexOf("\r\n\r\n") + 4);
            doc = Jsoup.parse(recordBody);
            return Optional.of(doc);
        } catch (org.jsoup.UncheckedIOException e){
            return Optional.empty();
        }



//        if (data.contains("\r\n\r\n")){
//            recordBody = data.substring(data.indexOf("\r\n\r\n") + 4);
//            doc = Jsoup.parse(recordBody);
//            System.out.println("TTTTTTrue");

////            return doc;
//        } else {
//
//            recordBody = data;
//            doc = Jsoup.parse(recordBody);
////            System.out.println("🐶 raw data: "+data);
////            System.out.print(data);
//            System.out.println("NNNNNNNO BLANK LINE");
////            return doc;
////            recordBody = data;
//        }
//
//        return doc;
//        String recordBody = data.substring(data.indexOf("\r\n\r\n") + 4);
//        Document doc = Jsoup.parse(recordBody);
    }


    private static String getNewestFileName(){
        String bucketName = "commoncrawl";

        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
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