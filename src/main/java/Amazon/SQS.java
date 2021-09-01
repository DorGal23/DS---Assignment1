package Amazon;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.*;


public class SQS {

    private final SqsClient sqs;
    private final String qName;
    private final String qURL;
    private final Region region = Region.US_EAST_1;


    public SQS(String queueName) {
        String url;
        this.sqs = SqsClient.builder().region(region).build();

        try {
            url = getQueueUrl(queueName);
            System.out.println("Connected to Queue: "+queueName);

        } catch (QueueDoesNotExistException e) {
            createQueue(queueName);
            url = getQueueUrl(queueName);
            System.out.println("New Queue: "+queueName);
        }
        this.qName = queueName;
        this.qURL = url;
    }

    private void createQueue(String queue_name) {
        sqs.createQueue(CreateQueueRequest.builder().queueName(queue_name)
                .build());
    }

    private String getQueueUrl(String queue_name) {
        return sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queue_name)
                .build())
                .queueUrl();
    }

    synchronized public void sendMessage(SendMessageRequest.Builder builder) {
        sqs.sendMessage(builder.queueUrl(qURL)
                .build());
    }

    public List<Message> nextMessages(int visibilityTimeout, int maxNumberOfMessages) {
        ReceiveMessageResponse response = receiveMessage(visibilityTimeout, maxNumberOfMessages);

        while (response.messages().isEmpty())
            response = receiveMessage(visibilityTimeout, maxNumberOfMessages);
        return response.messages();
    }

    private ReceiveMessageResponse receiveMessage(int visibilityTimeout, int maxNumberOfMessages) {
        return sqs.receiveMessage(ReceiveMessageRequest
                .builder()
                .queueUrl(qURL)
                .messageAttributeNames("All")
                .maxNumberOfMessages(maxNumberOfMessages)
                .waitTimeSeconds(10)                        // how long to wait for a message to get to the queue before returning
                .visibilityTimeout(visibilityTimeout)       // how long will the received messages be hidden from others with access to the sqs
                .build());
    }

    public void deleteMessage(Message message) {
        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(qURL).receiptHandle(message.receiptHandle())
                .build());
    }

    public void deleteSQS() {
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(qURL)
                .build());
        System.out.println("Queue Deleted: "+qName);
    }
}
