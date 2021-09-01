package Elements;

import Amazon.*;

import org.apache.commons.io.FileUtils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.*;
import java.util.*;

public class Manager {

    private static LinkedList<EC2> workersList;
    private static Map<String, PrintWriter> mapReports;
    private static Map<String, String> mapMissions;
    private static Map<String, Integer> mapAssignments;
    private static Map<String, String> mapTerminate;
    private static int assignmentsToDo;
    private static int missionID;
    private static int assignmentID;


    private static boolean terminated, flag;

    public static void main(String[] args) throws IOException, ParseException, InterruptedException{
        workersList = new LinkedList<>();
        mapReports = new HashMap<>();
        mapMissions = new HashMap<>();
        mapAssignments = new HashMap<>();
        mapTerminate = new HashMap<>();

        missionID = 1;
        assignmentID = 1;
        assignmentsToDo = 0;
        flag = false;
        terminated = false;

        FromLocal fromLocal = new FromLocal();
        ToWorkers toWorkers = new ToWorkers();

        Thread fromLocalThread = new Thread(fromLocal);
        Thread toWorkersThread = new Thread(toWorkers);

        fromLocalThread.start();
        toWorkersThread.start();

        toWorkersThread.join();
        fromLocal.managerToWorkers.deleteSQS();
    }

    public static class FromLocal implements Runnable{

        private final S3 s3;
        private final SQS localToManager;
        protected final SQS managerToWorkers;

        public FromLocal(){
            s3 = new S3("dor-bucket");
            localToManager = new SQS("localToManager");
            managerToWorkers = new SQS("managerToWorkers");
        }
        @Override
        public void run(){
            while (!terminated){
                try{
                    Message missionMessage = localToManager.nextMessages(120,1).get(0); // 2 min
                    newMission(missionMessage);
                    localToManager.deleteMessage(missionMessage);
                } catch (Exception e) {
                    if (!terminated) {
                        System.err.println("Unexpected Error: " + e.getMessage());
                        e.printStackTrace(System.err);
                    }
                }
            }
        }

        private void newMission(Message missionMessage) throws ParseException, IOException{
            String inputFile = missionMessage.messageAttributes().get("InputFileName").stringValue();
            String missionID = missionMessage.messageAttributes().get("missionID").stringValue();
            int numOfWorkers = Integer.parseInt(missionMessage.messageAttributes().get("NumOfWorkers").stringValue());
            boolean terminateFlag = Boolean.parseBoolean(missionMessage.messageAttributes().get("Termination").stringValue());

            String missionID1=missionID;
            s3.downloadFile(inputFile, "InputFile-" + missionID1);
            mapReports.put(missionID1, new PrintWriter(new FileWriter("ReviewsSummary-" + missionID1)));
            mapMissions.put(missionID1, missionID);
            if(terminateFlag){
                mapTerminate.put(missionID1, missionID);
            }

            LinkedList<String> assignments = newAssignments("InputFile-" + missionID1);
            int numOfAssignments = assignments.size();
            mapAssignments.put(missionID1, numOfAssignments);
            synchronized (this){
                assignmentsToDo+=numOfAssignments;
            }

            int maxWorkers = (int) Math.ceil((double) assignmentsToDo / (double) numOfWorkers) - workersList.size();
            for (int i=0; i<Math.min(maxWorkers, 15 - workersList.size()); i++) {
                System.out.println("Worker "+(i+1)+" Created");
                EC2 worker = new EC2(s3.getBucketName(), "Worker");
                workersList.add(worker);
            }
            for (String assignment : assignments)
                distributeAssignments(missionID1, assignment);
        }

        private LinkedList<String> newAssignments(String file) throws IOException, ParseException{
            LinkedList<String> assignments = new LinkedList<>();

            JSONParser parser = new JSONParser();
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = reader.readLine();
            while (line != null){
                JSONArray reviews = (JSONArray) ((JSONObject) parser.parse(line)).get("reviews");
                for (Object o : reviews) assignments.add(((JSONObject) o).toJSONString());
                line = reader.readLine();
            }
            reader.close();
            return assignments;
        }

        private void distributeAssignments(String missionID, String assignment){
            Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
            MessageAttributeValue printAttribute = MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue("Assignment-"+assignmentID++)
                    .build();
            MessageAttributeValue senderAttribute = MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue(missionID)
                    .build();
            messageAttributes.put("Print", printAttribute);
            messageAttributes.put("Sender", senderAttribute);


            managerToWorkers.sendMessage(SendMessageRequest.builder()
                    .messageBody(assignment)
                    .messageAttributes(messageAttributes)
            );
        }
    }

    public static class ToWorkers implements Runnable{

        private final S3 s3;
        private final SQS managerToLocal;
        private final SQS workersToManager;

        public ToWorkers(){
            s3 = new S3("dor-bucket");
            managerToLocal = new SQS("managerToLocal");
            workersToManager = new SQS("workersToManager");
        }

        @Override
        public void run(){
            while (!flag){
                List<Message> assignments = workersToManager.nextMessages(120,5); // 2 min

                for (Message assignment : assignments){
                    try{
                        assignmentComplete(assignment);
                        workersToManager.deleteMessage(assignment);
                    } catch (Exception e) {
                        System.err.println("Unexpected Error:"+e.getMessage());
                        if (!flag) {
                            terminated = true;
                        }
                        break;
                    }
                }
            }
            workersToManager.deleteSQS();
        }

        private void assignmentComplete(Message report){

            String toPrint = report.messageAttributes().get("Print").stringValue();
            String client = report.messageAttributes().get("Sender").stringValue();
            System.out.println(toPrint);

            mapReports.get(client).println(report.body());
            mapAssignments.put(client, mapAssignments.get(client) - 1);
            synchronized (this){
                assignmentsToDo-=1;
            }

            if (mapAssignments.get(client) == 0){
                missionComplete(client);
            }
        }

        private void missionComplete(String missionId){
            mapReports.get(missionId).close();
            mapReports.remove(missionId);
            FileUtils.deleteQuietly(new File("InputFile-" + missionId));

            s3.uploadFile("ReviewsSummary-" + missionId, "ReviewsSummary-" + mapMissions.get(missionId));
            FileUtils.deleteQuietly(new File("ReviewsSummary-" + missionId));
            mapAssignments.remove(missionId);

            if (mapTerminate.containsKey(missionId)){
                terminated = true;
            }
            sendMissionCompleted(missionId);
            mapMissions.remove(missionId);

            if (terminated && assignmentsToDo == 0) {
                terminateWorkers();
                sendTerminationMessage(missionId);
                flag = true;
            }
        }

        private void sendMissionCompleted(String missionId){
            Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
            MessageAttributeValue printAttribute = MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue("Summary Completed: " + mapMissions.get(missionId))
                    .build();
            MessageAttributeValue identifierAttribute = MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue(mapMissions.get(missionId))
                    .build();
            MessageAttributeValue reviewsSummaryAttribute = MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue("ReviewsSummary-" + mapMissions.get(missionId))
                    .build();
            MessageAttributeValue missionIDAttribute = MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue(mapMissions.get(missionId))
                    .build();
            messageAttributes.put("Print", printAttribute);
            messageAttributes.put("Identifier", identifierAttribute);
            messageAttributes.put("ReviewsSummary", reviewsSummaryAttribute);
            messageAttributes.put("missionID", missionIDAttribute);

            managerToLocal.sendMessage(SendMessageRequest.builder()
                    .messageBody("Completed")
                    .messageAttributes(messageAttributes)
            );
        }

        private void terminateWorkers(){
            for (EC2 worker : workersList) {
                System.out.println("Terminating worker: "+worker.getInstanceId());
                worker.terminate();
            }
        }

        private void sendTerminationMessage(String missionId){
            Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
            MessageAttributeValue printAttribute = MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue("Terminated")
                    .build();
            MessageAttributeValue identifierAttribute = MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue(mapTerminate.get(missionId))
                    .build();
            MessageAttributeValue terminateAttribute = MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue("Terminate")
                    .build();
            messageAttributes.put("Print", printAttribute);
            messageAttributes.put("Identifier", identifierAttribute);
            messageAttributes.put("Terminate", terminateAttribute);

            managerToLocal.sendMessage(SendMessageRequest.builder()
                    .messageBody("Terminate")
                    .messageAttributes(messageAttributes)
                    .delaySeconds(10)
            );
        }
    }

    public static String getUserData(String bucketName){
        String cmd = "#!/bin/bash"+'\n'+
                "wget https://"+bucketName+".s3.amazonaws.com/Manager.jar"+'\n'+
                "java -jar Manager.jar"+'\n';
        return Base64.getEncoder().encodeToString(cmd.getBytes());
    }
}