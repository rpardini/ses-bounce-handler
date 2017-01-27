<?php
require dirname(__FILE__) . '/vendor/autoload.php';

date_default_timezone_set('UTC');
use Aws\Sqs\SqsClient;
use markdunphy\SesSnsTypes\Notification\MessageTypeFactory;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;


class BadMessageException extends \Exception
{
}


$command = new \Commando\Command();
$command->flag('mode')->alias("runMode")->describedAs("Mode to run in. Could be 'poll', 'postfix', or the default which is 'both'.")->defaultsTo("both");
$command->flag('region')->alias("awsRegion")->describedAs("AWS region where SES and SQS queues are, eg, us-east-1")->required();
$command->flag('access')->alias("accessKey")->describedAs("AWS access key for the queue")->required();
$command->flag('secret')->alias("secretKey")->describedAs("AWS secret key for the queue")->required();
$command->flag('domain')->alias("mailDomain")->describedAs("The mail domain for which to receive bounces, will be used to compose queue name.")->required();
$command->flag('mongo')->alias("mongoHost")->alias("mongoDbHost")->describedAs("The MongoDB hostname where we will store bounces and bans.")->required();
$command->flag('bounces')->alias("bouncesCollection")->describedAs("The MongoDB collection that will store all complaints and bounces in their full form. Defaults to 'bounces'.")->defaultsTo("bounces");
$command->flag('banned')->alias("bannedCollection")->describedAs("The MongoDB collection that will store banned addresses with unique key on email. Defaults to 'banned'.")->defaultsTo("banned");
$command->flag('mongoDatabase')->alias("db")->describedAs("The MongoDB database name to use. Defaults to 'mailbounces'.")->defaultsTo("mailbounces");


$sqsReqion = $command['region'];
$sqsAccessKey = $command['access'];
$sqsSecretKey = $command['secret'];
$mailDomain = $command['domain'];
$mongoDbHost = $command['mongo'];
$mongoDbBouncesCollection = $command['bounces'];
$mongoDbBannedCollection = $command['banned'];
$mongoDbDb = $command['mongoDatabase'];
$runMode = strtolower($command['mode']);

// If any options are missing, Commando will have thrown an Exception above.
$log = new Logger('ses-bounce-handler');
$log->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));
$log->debug("Running with parameters", array($command->getFlagValues()));

$newBans = 0;
if ($runMode == "both" || $runMode == "poll") {
    $newBans = getMessagesFromSQS($sqsReqion, $sqsAccessKey, $sqsSecretKey, $mailDomain, $log, $mongoDbHost, $mongoDbDb, $mongoDbBouncesCollection, $mongoDbBannedCollection);
}

if ($runMode == "both" || $runMode == "postfix") {
    if (($newBans > 0) || $runMode == "postfix") {
        $log->info("Got total {$newBans} bans, let's update postfix ban list.");
        $postfixDbFilename = "transport_banned";
        updatePostfixDBFromMongo($log, $mongoDbHost, $mongoDbDb, $mongoDbBannedCollection, $postfixDbFilename);
    } else {
        $log->info("No new bans added, not updating postfix.");
    }
}

/**
 * @param $log Logger
 * @param $mongoDbHost string
 * @param $mongoDbDb string
 * @param $mongoDbBannedCollection string
 */
function updatePostfixDBFromMongo($log, $mongoDbHost, $mongoDbDb, $mongoDbBannedCollection, $postfixDbFilename) {
    $mongoClient = new MongoDB\Client("mongodb://${mongoDbHost}/");
    $bannedCollection = $mongoClient->selectDatabase($mongoDbDb)->selectCollection($mongoDbBannedCollection);
    $cursor = $bannedCollection->find();

    $records = array();
    foreach ($cursor as $ban) {
        $email = $ban['email'];
        $timestamp = $ban['timestamp'];
        $reason = $ban['reason'];
        var_dump($ban);
        $record = "{$email} discard:BANNED # {$reason} {$timestamp}";
        $records[] = $record;
    }

    // Done, write the file.
    file_put_contents($postfixDbFilename, implode("\n", $records));

    // then call postmap, if available.
    // then reload postfix to make sure it picks up on the new map.
}


/**
 * @param $sqsReqion string
 * @param $sqsAccessKey string
 * @param $sqsSecretKey string
 * @param $mailDomain string
 * @param $log Logger
 * @param $mongoDbHost string
 * @param $mongoDbDb string
 * @param $mongoDbBouncesCollection string
 * @param $mongoDbBannedCollection string
 * @return int the total number of added/updated bans
 */
function getMessagesFromSQS($sqsReqion, $sqsAccessKey, $sqsSecretKey, $mailDomain, $log, $mongoDbHost, $mongoDbDb, $mongoDbBouncesCollection, $mongoDbBannedCollection)
{
    $messageTypeFactory = new MessageTypeFactory();

    $sqsClient = SqsClient::factory([
        'version' => 'latest',
        'region' => $sqsReqion,
        'credentials' => [
            'key' => $sqsAccessKey,
            'secret' => $sqsSecretKey,
        ]
    ]);

    $queueName = str_replace(".", "_", $mailDomain . '-bounce-queue');

    $result = $sqsClient->getQueueUrl(array('QueueName' => $queueName));
    $queueUrl = $result->get('QueueUrl');

    $log->info("Queue URL", array('queueUrl' => $queueUrl, 'queueName' => $queueName));

    $maxMessagesToReceive = 2;
    $couldHaveMoreMessages = true;
    $addedBans = 0;

    while ($couldHaveMoreMessages) {
        $result = $sqsClient->receiveMessage(array(
            'QueueUrl' => $queueUrl,
            'MaxNumberOfMessages' => $maxMessagesToReceive,
            'WaitTimeSeconds' => 20,
        ));

        $messagesReceived = 0;

        if (!($result['Messages'] == null)) {
            $log->info("Got some messages in queue.");
            foreach ($result['Messages'] as $messageObj) {
                $log->info("Got a message from SQS.", array('MessageId' => $messageObj['MessageId']));
                $messagesReceived++;
                $receiptHandle = $messageObj['ReceiptHandle'];

                try {
                    $jsonBody = json_decode($messageObj['Body']);

                    $notificationMessage = $jsonBody->Message;
                    if (!$notificationMessage) {
                        // Not a valid JSON body, delete it.
                        throw new BadMessageException("Message Body is invalid JSON.");
                    }

                    try {
                        $notification = $messageTypeFactory->create($notificationMessage);

                        // Insert the full bounce/complaint into MongoDB for future reference. Or not.
                        if (isset($mongoDbHost) && isset($mongoDbDb) && isset($mongoDbBouncesCollection)) {
                            $mongoClient = new MongoDB\Client("mongodb://${mongoDbHost}/");
                            $collection = $mongoClient->selectDatabase($mongoDbDb)->selectCollection($mongoDbBouncesCollection);
                            $bounceInserted = $collection->insertOne($notification->getFullPayloadAsArray());
                            $log->info("Logged full bounce/complaint to MongoDB with ID " . $bounceInserted->getInsertedId());
                            unset($mongoClient);
                            unset($collection);
                        }

                        // Typing system is idiotic, these are only to help me with IDE completion
                        /** @var \markdunphy\SesSnsTypes\Notification\BounceMessage $bounce */
                        $bounce = $notification;

                        /** @var \markdunphy\SesSnsTypes\Notification\ComplaintMessage $complaint */
                        $complaint = $notification;

                        /** @var \markdunphy\SesSnsTypes\Entity\RecipientInterface[] $recipients */
                        $recipients = null;

                        $reason = null;
                        $badEnoughForBanning = false;
                        $timestamp = null;

                        if ($bounce->isBounceNotification()) {
                            $recipients = $bounce->getBouncedRecipients();
                            $badEnoughForBanning = $bounce->isHardBounce() || $bounce->isUndeterminedBounce(); // Only hard and undetermined bounces are bad enough
                            $timestamp = $bounce->getBouncedTimestamp();
                            $reason = "Bounce {$bounce->getBounceMessage()} {$bounce->getBounceSubType()} from {$bounce->getReportingMTA()}";
                        } elseif ($complaint->isComplaintNotification()) {
                            $recipients = $complaint->getComplainedRecipients();
                            $badEnoughForBanning = true; // complaints are ALWAYS bad enough
                            $timestamp = $complaint->getComplaintTimestamp();
                            $reason = "Complaint {$complaint->getUserAgent()} {$complaint->getComplaintFeedbackType()}";
                        }

                        if ($badEnoughForBanning) {
                            $parsedTS = new DateTime($timestamp);

                            $mongoClient = new MongoDB\Client("mongodb://${mongoDbHost}/");
                            $bannedCollection = $mongoClient->selectDatabase($mongoDbDb)->selectCollection($mongoDbBannedCollection);
                            // Make sure the collection has an unique index on email.
                            $bannedCollection->createIndex(['email' => 1], ['unique' => true]);


                            foreach ($recipients as $recipient) {
                                $rcptEmail = $recipient->getEmailAddress();
                                $log->info("Will ban recipient with timestamp.", array('timestamp' => $parsedTS, 'email' => $rcptEmail));
                                $docToInsert = array('email' => $rcptEmail, 'timestamp' => new \MongoDB\BSON\UTCDatetime($parsedTS), 'reason' => $reason);

                                $upsert = $bannedCollection->updateOne(['email' => $rcptEmail], ['$set' => $docToInsert], ['upsert' => true]);
                                $log->info("Upserted ban in MongoDb with ID " . $upsert->getUpsertedId());
                                $addedBans++;
                            }

                            unset($bannedCollection);
                            unset($mongoClient);
                        }

                        // suppose everything went well, remove message from queue.
                        $sqsClient->deleteMessage(array('QueueUrl' => $queueUrl, 'ReceiptHandle' => $receiptHandle));

                    } catch (\markdunphy\SesSnsTypes\Exception\MalformedPayloadException $e) {
                        // Message is malformed, means it's not something we're expecting here, so delete it.
                        throw new BadMessageException("Unable to parse message SES notification: " . $e->getMessage());
                    } catch (Exception $e) {
                        // Any other exception, we should NOT delete the message.
                        $log->emerg("Fatal error during handling of SQS message. {$e->getMessage()}", array('exception' => $e, 'message' => $messageObj));
                    }


                } catch (BadMessageException $e) {
                    $log->error("Invalid notification message received. Ignore and delete.", array('err' => $e->getMessage(), 'messageId' => $messageObj['MessageId']));
                    $sqsClient->deleteMessage(array('QueueUrl' => $queueUrl, 'ReceiptHandle' => $receiptHandle));
                }
            }
        }

        $couldHaveMoreMessages = $messagesReceived > 0; // ($messagesReceived == $maxMessagesToReceive);
        $log->info("Messages processed ${messagesReceived}.");
    }

    $log->info("Done processing SQS messages. Added {$addedBans} bans.");
    return $addedBans;
}
