package Elements;

import Amazon.*;

import org.apache.commons.io.FileUtils;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONObject;

import java.awt.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static j2html.TagCreator.*;

public class LocalApplication {

    public static void main(String[] args) throws IOException, ParseException{
        if (args.length < 2){
            System.out.println("Not enough Args");
            System.exit(1);
        }
        String inputFileName = args[0];
        String outputFileName = args[1];
        int numOfWorkers = Integer.parseInt(args[2]);
        boolean shouldTerminate = (args.length > 3 && args[3].equals("terminate"));
        long inceptionTime = System.currentTimeMillis();     //get start time for measuring purposes

        run(inputFileName, outputFileName, numOfWorkers, shouldTerminate);

        long terminationTime = System.currentTimeMillis();
        long runTime = terminationTime - inceptionTime;
        long minutes = TimeUnit.MILLISECONDS.toMinutes(runTime);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(runTime) % 60;
        System.out.println("Task time: "+minutes+" minutes and "+seconds+" seconds");
    }

    private static void run(String inputFileName, String outputFileName, int numOfWorkers, boolean terminate) throws IOException, ParseException{
        String id = Long.toString(System.currentTimeMillis());      // id as a function of time
        S3 s3 = new S3("dor-bucket");
        SQS localToManager = new SQS("localToManager");
        SQS managerToLocal = new SQS("managerToLocal");

        //s3.uploadFile("out/artifacts/Manager/Manager.jar", "Manager.jar");
        //s3.uploadFile("out/artifacts/Worker/Worker.jar", "Worker.jar");
        s3.uploadFile("Input_Files/" + inputFileName + ".txt", "InputFile-" + id);

        EC2 manager = new EC2(s3.getBucketName(), "Manager");

        sendAnalysisRequest(localToManager, id, numOfWorkers, terminate);
        System.out.println("Mission Delivered Successfully");

        Message m1 = getMessage(id, managerToLocal);
        String sourceFile = m1.messageAttributes().get("ReviewsSummary").stringValue();
        managerToLocal.deleteMessage(m1);

        s3.downloadFile(sourceFile, "ReviewsSummary-" + id);
        s3.deleteFile(sourceFile);

        System.out.println("Creating HTML Reviews Summary for: " + "ReviewsSummary-missionID-" + id);
        makeListOfReviews("ReviewsSummary-" + id, outputFileName);

        s3.deleteFile("InputFile-" + id);
        FileUtils.deleteQuietly(new File("ReviewsSummary-" + id));

        if (terminate){
            terminate(id, managerToLocal, localToManager, s3, manager);
        }

        File htmlFile = new File(outputFileName + ".html");
        Desktop.getDesktop().browse(htmlFile.toURI());
    }

    private static Message getMessage(String id, SQS managerToLocal){
        Message response = managerToLocal.nextMessages(10,1).get(0); // 10 sec
        while (!response.messageAttributes().containsKey("Identifier")
                || !response.messageAttributes().get("Identifier").stringValue().equals("missionID-" + id))
            response = managerToLocal.nextMessages(10,1).get(0); // 10 sec
        return response;
    }

    private static void sendAnalysisRequest(SQS localToManager, String id, int numOfWorkers, boolean terminate){
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        MessageAttributeValue printAttribute = MessageAttributeValue.builder()
                .dataType("String")
                .stringValue("New Mission from Local-" + id)
                .build();
        MessageAttributeValue inputFileNameAttribute = MessageAttributeValue.builder()
                .dataType("String")
                .stringValue("InputFile-"+id)
                .build();
        MessageAttributeValue missionIDAttribute = MessageAttributeValue.builder()
                .dataType("String")
                .stringValue("missionID-"+id)
                .build();
        MessageAttributeValue numOfWorkersAttribute = MessageAttributeValue.builder()
                .dataType("String")
                .stringValue(Integer.toString(numOfWorkers))
                .build();
        MessageAttributeValue terminateAttribute = MessageAttributeValue.builder()
                .dataType("String")
                .stringValue(Boolean.toString(terminate))
                .build();
        messageAttributes.put("Print", printAttribute);
        messageAttributes.put("InputFileName", inputFileNameAttribute);
        messageAttributes.put("missionID", missionIDAttribute);
        messageAttributes.put("NumOfWorkers", numOfWorkersAttribute);
        messageAttributes.put("Termination", terminateAttribute);

        localToManager.sendMessage(SendMessageRequest.builder()
                .messageBody("Analysis Request to the Manager")
                .messageAttributes(messageAttributes)
        );
    }

private static void makeListOfReviews(String source, String output) throws IOException, ParseException{

    BufferedReader sourceReader = new BufferedReader(new FileReader(source));
    JSONParser parser = new JSONParser();
    Object parsedJson;
    String json = sourceReader.readLine();

    LinkedList<Review> reviews = new LinkedList<>();
    while (json != null) {
        parsedJson = parser.parse(json);
        JSONObject revInfo = (JSONObject) parsedJson;
        String link = (String) revInfo.get("link");
        int sentiment = ((Long) revInfo.get("sentiment")).intValue();
        int rating = ((Long) revInfo.get("rating")).intValue();
        String entity = revInfo.get("entities").toString();
        reviews.add(new Review(link, sentiment, rating, entity));
        json = sourceReader.readLine();
    }
    sourceReader.close();
    makeHtmlFile(reviews, output);
}

    private static void makeHtmlFile(LinkedList<Review> reviews,  String output) throws IOException{

        String summary = html().with(head(
                title("All Amazon Reviews"),
                link().withRel("stylesheet").withHref("/css/main.css")),
                body(
                        h1("DSPS Assignment 1: Amazon reviews"),
                        br(),
                        table(tr(th("Link to original review"),
                                th("Entities"),
                                th("Sarcasm?")),
                                each(reviews, review ->
                                        tr(td(a(review.getLink().split("/ref")[0])
                                                        .withStyle("color:" + review.getColor())
                                                        .withTarget("_blank")
                                                        .withHref(review.getLink())),
                                                td(review.getEntity()),
                                                td(b(review.isSarcastic(review.getSentiment(), review.getRating()))))))))
                .renderFormatted();

        FileUtils.deleteQuietly(new File(output));
        BufferedWriter writer = new BufferedWriter(new FileWriter(output + ".html"));
        writer.write(summary);
        writer.close();
    }



    private static void terminate(String id, SQS managerToLocal,SQS localToManager, S3 s3, EC2 manager){
        Message m = getMessage(id, managerToLocal);
        managerToLocal.deleteMessage(m);
        s3.deleteBucket();
        managerToLocal.deleteSQS();
        localToManager.deleteSQS();
        manager.terminate();
    }

    public static class Review{
        private final String link;
        private final Integer sentiment;
        private final Integer rating;
        private final String entity;

        public Review(String link, Integer sentiment, Integer rating, String entity){
            this.link = link;
            this.sentiment = sentiment;
            this.rating = rating;
            this.entity = entity;
        }

        public String getLink() {return link;}
        public Integer getSentiment() {return sentiment;}
        public Integer getRating() {return rating;}
        public String getEntity() {return entity;}

        public String isSarcastic(int sentiment, int rating) {
            if (sentiment+1 == rating || sentiment == rating || sentiment-1 == rating ||sentiment-2 == rating || sentiment + 2 == rating)
                return "Not Sarcastic";
            return "Sarcastic";
        }

        public String getColor(){
            switch (this.sentiment){
                case 0:
                    return ("#b30000");      // Dark Red
                case 1:
                    return ("#ff4d4d");      // Red
                case 2:
                    return ("#000000");      // Black
                case 3:
                    return ("#4dff4d");      // Light Green
                case 4:
                    return ("#00e673");      // Dark Green
                default:
                    return null;
            }
        }
    }
}

