<?php
const ETC_POSTFIX_TRANSPORT_BANNED = "/etc/postfix/transport_banned";
require dirname(__FILE__) . '/vendor/autoload.php';

date_default_timezone_set('UTC');
use Aws\Sqs\SqsClient;
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
$command->flag('quiet')->alias("cron")->describedAs("Try to be very silent, not logging anything unless there's an error.")->boolean()->defaultsTo(false);


$sqsReqion = $command['region'];
$sqsAccessKey = $command['access'];
$sqsSecretKey = $command['secret'];
$mailDomain = $command['domain'];
$mongoDbHost = $command['mongo'];
$mongoDbBouncesCollection = $command['bounces'];
$mongoDbBannedCollection = $command['banned'];
$mongoDbDb = $command['mongoDatabase'];
$runMode = strtolower($command['mode']);
$logQuiet = $command['quiet'];

// If any options are missing, Commando will have thrown an Exception above.
$log = new Logger('ses-bounce-handler');
$consoleHandler = new StreamHandler('php://stdout', Logger::DEBUG);
if ($logQuiet) {
    $log->pushHandler(new \Monolog\Handler\FingersCrossedHandler($consoleHandler, Logger::WARNING));
} else {
    $log->pushHandler($consoleHandler);
}
$log->debug("Running with parameters", array($command->getFlagValues()));

$newBans = 0;
if ($runMode == "both" || $runMode == "poll") {
    $newBans = getMessagesFromSQS($sqsReqion, $sqsAccessKey, $sqsSecretKey, $mailDomain, $log, $mongoDbHost, $mongoDbDb, $mongoDbBouncesCollection, $mongoDbBannedCollection);
}

/**
 * @return bool
 */
function isRunningOnPostfixSystem()
{
    return file_exists(ETC_POSTFIX_TRANSPORT_BANNED);
}

if ($runMode == "both" || $runMode == "postfix") {
    if (($newBans > 0) || $runMode == "postfix") {
        $log->info("Got total {$newBans} bans, let's update postfix ban list.");
        updatePostfixDBFromMongo($log, $mongoDbHost, $mongoDbDb, $mongoDbBannedCollection);
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
function updatePostfixDBFromMongo($log, $mongoDbHost, $mongoDbDb, $mongoDbBannedCollection)
{
    $mongoClient = new MongoDB\Client("mongodb://${mongoDbHost}/");
    $bannedCollection = $mongoClient->selectDatabase($mongoDbDb)->selectCollection($mongoDbBannedCollection);
    $cursor = $bannedCollection->find();

    $records = array();
    foreach ($cursor as $ban) {
        $email = $ban['email'];
        /** @var \MongoDB\BSON\UTCDatetime $timestamp */
        $timestamp = $ban['timestamp'];
        $reason = $ban['reason'];
        /** @noinspection PhpVoidFunctionResultUsedInspection */
        /** @noinspection PhpUndefinedMethodInspection */
        $record = "{$email} discard:BANNED {$reason} at {$timestamp->toDateTime()->format('Y-m-d H:i:s')}";
        $records[] = $record;
    }

    // Done, write the file.
    $postfixDbFilename = isRunningOnPostfixSystem() ? ETC_POSTFIX_TRANSPORT_BANNED : dirname(__FILE__) . DIRECTORY_SEPARATOR . "transport_banned";
    $log->info("Writing banned database to $postfixDbFilename");
    file_put_contents($postfixDbFilename, implode("\n", $records));

    if (isRunningOnPostfixSystem()) {
        $log->info("Running postmap.");
        exec("/usr/sbin/postmap " . ETC_POSTFIX_TRANSPORT_BANNED);

        $log->info("Reloading postfix so it picks up the new transport map.");
        exec("/usr/sbin/service postfix reload");
    }
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
    $messageTypeFactory = new PHP55CompatibleMessageTypeFactory();

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
                        $log->info("Message deleted from SQS.");

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


/****
 * COMPLETE BULLSHIT, just to remove array usage in constants so this shit can be used with PHP 5.5
 */
class PHP55CompatibleMessageTypeFactory
{
    /**
     * @param string|array $payload json string or array of SNS payload data
     * @return \markdunphy\SesSnsTypes\Notification\MessageTypeInterface
     * @throws \markdunphy\SesSnsTypes\Exception\InvalidTypeException
     * @throws \markdunphy\SesSnsTypes\Exception\MalformedPayloadException
     */
    public function create($payload)
    {

        if (is_string($payload)) {
            $payload = json_decode($payload, true);
        }

        if (!is_array($payload)) {
            throw new \markdunphy\SesSnsTypes\Exception\InvalidTypeException('Argument 1 for NotificationMessageTypeFactory::create must be valid JSON string or array');
        }

        if (!PHP55CompatiblePayloadValidator::isValid($payload)) {
            throw new \markdunphy\SesSnsTypes\Exception\MalformedPayloadException('Supplied SNS payload is malformed');
        }

        $TYPE_CLASSES_BY_STRING = [
            'bounce' => \markdunphy\SesSnsTypes\Notification\BounceMessage::class,
            'complaint' => \markdunphy\SesSnsTypes\Notification\ComplaintMessage::class,
            'delivery' => \markdunphy\SesSnsTypes\Notification\DeliveryMessage::class,
        ];
        $class = $TYPE_CLASSES_BY_STRING[strtolower($payload['notificationType'])];
        return new $class($payload);
    }

}

/****
 * COMPLETE BULLSHIT, just to remove array usage in constants so this shit can be used with PHP 5.5
 */
class PHP55CompatiblePayloadValidator
{


    /**
     * @param array $payload
     * @return bool
     */
    public static function isValid(array $payload)
    {
        $REQUIRED_TOP_LEVEL_FIELDS = [
            'notificationType',
            'mail',
        ];

        $REQUIRED_MAIL_FIELDS = [
            'timestamp',
            'messageId',
            'source',
            'sourceArn',
            'sendingAccountId',
            'destination',
        ];

        $REQUIRED_BOUNCE_FIELDS = [
            'bounceType',
            'bounceSubType',
            'bouncedRecipients',
            'timestamp',
            'feedbackId',
        ];

        $REQUIRED_COMPLAINT_FIELDS = [
            'complainedRecipients',
            'timestamp',
            'feedbackId',
        ];

        $REQUIRED_DELIVERY_FIELDS = [
            'timestamp',
            'processingTimeMillis',
            'recipients',
            'smtpResponse',
            'reportingMTA',
        ];

        $NOTIFICATION_TYPE_VALIDATORS = [
            'bounce' => $REQUIRED_BOUNCE_FIELDS,
            'complaint' => $REQUIRED_COMPLAINT_FIELDS,
            'delivery' => $REQUIRED_DELIVERY_FIELDS,
        ];


        foreach ($REQUIRED_TOP_LEVEL_FIELDS as $field) {
            if (!isset($payload[$field])) {
                return false;
            }
        }

        $type = strtolower($payload['notificationType']);

        if (!isset($payload[$type])) {
            return false;
        }

        if (!static::isObjectValid($payload['mail'], $REQUIRED_MAIL_FIELDS)) {
            return false;
        }

        if (!static::isObjectValid($payload[$type], $NOTIFICATION_TYPE_VALIDATORS[$type])) {
            return false;
        }

        return true;

    }

    /**
     * @param array $object
     * @param $expectedFields
     * @return bool
     */
    private static function isObjectValid(array $object, $expectedFields)
    {

        foreach ($expectedFields as $field) {
            if (!isset($object[$field])) {
                return false;
            }
        }

        return true;

    }

}