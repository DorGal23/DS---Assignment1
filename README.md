# DS---Assignment1
Nitay Marciano : 203343132
Dor Gal : 308575588
Assignment 1

Structure:
In this project, we used three different elements, three AWS services, and two special algorithms from Stanford.
Elements:

LocalApplication – The client who sends new missions to the manager.

Manager – Manages, it is the “pipe” between the LocalApplication and the Workers.

Worker(s) – Do the “dirty” work. Get assignments from the manager and return them once they are done.
AWS services:

EC2 –Both the manager and workers are EC2 instances. We used the T2_LARGE instances (may be a bit costy, but more work-friendly)

S3 – Storage system. This is where the buckets are, where the JAR files, reviews, ect are.

SQS – Simple Queue Service. We have 4 different queues - To and from LocalApp and Manager, and to and from Manager and Workers.
Stanford:

NamedEntityRecognition – Helps retrieve names, locations, organizations from the reviews.

SentimentAnalasys – Helps understand if the review is positive or negative.
RUN:
Go to LocalApplication, edit configuration and enter inputfile (make sure it is under the folder of InputFiles, outputfile(your choice), how many reviews/assignments per worker and an optional argument if you want the LocalApplication to terminate the Manager (send the Manager a termination message).
Flow:
The Local Application first creates the bucket and the queues from and to the Manager, then uploads the input file to a bucket in S3. Now the Local Application initiates the Manager,
and from now it waits until it will receive from the Manager the reviews.
The Manager (an EC2 instance) was initiated, and receives the mission from the LocalApp  via the localToManager SQS queue. The Manager calculates how many Workers are needed and creates them (EC2 instances). Once created, the Workers receive assignments from the Manager via the managerToWorkers SQS queue. Each worker then processes its delegated reviews, and once done it sends it back to the manager, through another the workersToManager SQS queue.
The Manager delivers the review almost as is (it uses another data structure to hold all the reviews) to the Local App, and the "hard" job of putting the review altogether to create the summary of the reviews is handled by the LocalApp.
The LocalApplication lastly makes an HTML that has a table of all the reviews, and before gracefully terminating it cleans the AWS (deleting queues, bucket etc..), and if needed also terminates the Manager.
AMI used:  ami-081475026498ccd01
Security
We realize that security is a very important factor, mainly because of all the open source code there is nowadays which enables others to reach this code as well. Therefore, if we do not save our credentials somewhere safe that only we can access, we are risking other people using our money! In this case, its not that much of a loss, but if we were a big company with many means, we could end up losing a lot of money!
We used AWS’s plugin which connects to your account only once you enter your credentials, and it is not a part of the project itself, rather only part of IntelliJ (which we used to program this). Meaning, we did not hardcopy our credentials! Therefore intruders had no way of accessing this.
In addition, we used the Identity and Access Management (IAM) in order to limit the access of the program to our cloud services to just what it needs in order to run smoothly.
Manager, Workers and Local Application (Client) responsibilities
For this project we understood that the best way to run everything fast and efficiently, is to make the manager do the least amount of work it possibly can and “focus” more on managing the workers and leaving all the work up to them. So we made sure that our manager would only deal with getting missions from LocalApp (clients), delegating assignments to Workers, receiving, sending, and holding reviews (but not processing them), and our workers will be used to the fullest! The workers are the ones that get the relevant review and process it completely, while the manager only sends the messages back and forth, acting as a sort of pipe between the localApp and the workers. The Local Application is the one responsible for organizing the reviews in a nice, readable form, which also saves time for the manager.


Scalability

As mentioned, during this project we didn’t have to deal with much. But there will of course be instances where we would want to use AWS for very big projects and data, therefore scalability is huge here. We thought about this, and took care of it from the Manager perspective by using  two threads – one for sending assignments to the workers, and the other responsible for dealing with the LocalApplication (receiving mission, and sending reviews etc). 
For extra scalability we would have created a thread for each LocalApplication (Client) to maximize the scalability rate. That way each client is not dependent on other clients, and for termination of the manager we keep a list of all the terminations and wait until they are all true to terminate the manager.
Additionally, we had the Client itself make the final report once it received all of the reviews analyzation, again not adding any unnecessary work load onto the manager.

Problems & Solutions
In our project, if one of the worker nodes dies or stalls we saw that it can cause problems, because it will lead to the job getting stuck and maybe even the whole program. Our solution was to use the visibility timeout attribute of the SQS. We gave each worker 800 seconds to complete 8 assignments (dependent on the maxNumberOfMessage), therefore if it took the Worker too long (in our opinion), the SQS will reveal these assignments to the other workers (it's important to notice that by using the visibility timeout we don’t remove messages from the SQS, only hide them from other Workers). In addition, since this Worker is slacking we would like to terminate it, and create a new one, this can be done by sending a message to the manager to terminate this worker, and create a new one instead.

Termination
There is an option to run this project and terminate all when done. In this case, we made sure we closed every instance – the bucket with all of its files, the queues themselves, and all the instances. This also adds a layer of security, as our instances will not exist once we are done running everything.


Notes
•	We limited the amount of Workers, as pointed by the lecturer

