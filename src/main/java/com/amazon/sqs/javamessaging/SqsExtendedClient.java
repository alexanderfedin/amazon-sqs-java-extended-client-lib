/**
 * 
 */
package com.amazon.sqs.javamessaging;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

/**
 * @author Alexander
 *
 * Amazon SQS Extended Client extends the functionality of Amazon SQS client.
 * All service calls made using this client are blocking, and will not return
 * until the service call completes.
 *
 * <p>
 * The Amazon SQS extended client enables sending and receiving large messages
 * via Amazon S3. You can use this library to:
 * </p>
 *
 * <ul>
 * <li>Specify whether messages are always stored in Amazon S3 or only when a
 * message size exceeds 256 KB.</li>
 * <li>Send a message that references a single message object stored in an
 * Amazon S3 bucket.</li>
 * <li>Get the corresponding message object from an Amazon S3 bucket.</li>
 * <li>Delete the corresponding message object from an Amazon S3 bucket.</li>
 * </ul>
 */
public class SqsExtendedClient extends SqsExtendedClientBase implements SqsClient {
	private static final Log LOG = LogFactory.getLog(AmazonSQSExtendedClient.class);

	private ExtendedClientConfiguration clientConfiguration;

	/**
	 * Constructs a new Amazon SQS extended client to invoke service methods on
	 * Amazon SQS with extended functionality using the specified Amazon SQS client
	 * object.
	 *
	 * <p>
	 * All service calls made using this new client object are blocking, and will
	 * not return until the service call completes.
	 *
	 * @param impl The Amazon SQS client to use to connect to Amazon SQS.
	 */
	public SqsExtendedClient(SqsClient sqsClient) {
		this(sqsClient, new ExtendedClientConfiguration());
	}


	/**
	 * Constructs a new Amazon SQS extended client to invoke service methods on
	 * Amazon SQS with extended functionality using the specified Amazon SQS client
	 * object.
	 *
	 * <p>
	 * All service calls made using this new client object are blocking, and will
	 * not return until the service call completes.
	 *
	 * @param impl            The Amazon SQS client to use to connect to Amazon
	 *                             SQS.
	 * @param extendedClientConfig The extended client configuration options
	 *                             controlling the functionality of this client.
	 */
	public SqsExtendedClient(SqsClient sqsClient, ExtendedClientConfiguration extendedClientConfig) {
		super(sqsClient);
		this.clientConfiguration = new ExtendedClientConfiguration(extendedClientConfig);
	}

	/**
	 * <p>
	 * Delivers a message to the specified queue and uploads the message payload to
	 * Amazon S3 if necessary.
	 * </p>
	 * <p>
	 * <b>IMPORTANT:</b> The following list shows the characters (in Unicode)
	 * allowed in your message, according to the W3C XML specification. For more
	 * information, go to http://www.w3.org/TR/REC-xml/#charsets If you send any
	 * characters not included in the list, your request will be rejected. #x9 | #xA
	 * | #xD | [#x20 to #xD7FF] | [#xE000 to #xFFFD] | [#x10000 to #x10FFFF]
	 * </p>
	 *
	 * <b>IMPORTANT:</b> The input object may be modified by the method.
	 * </p>
	 *
	 * @param sendMessageRequest Container for the necessary parameters to execute
	 *                           the SendMessage service method on AmazonSQS.
	 *
	 * @return The response from the SendMessage service method, as returned by
	 *         SqsClient.
	 *
	 * @throws InvalidMessageContentsException
	 * @throws UnsupportedOperationException
	 *
	 * @throws AmazonClientException           If any internal errors are
	 *                                         encountered inside the client while
	 *                                         attempting to make the request or
	 *                                         handle the response. For example if a
	 *                                         network connection is not available.
	 * @throws AmazonServiceException          If an error response is returned by
	 *                                         AmazonSQS indicating either a problem
	 *                                         with the data in the request, or a
	 *                                         server side issue.
	 */
	public SendMessageResponse sendMessage(SendMessageRequest sendMessageRequest) {

		if (sendMessageRequest == null) {
			String errorMessage = "sendMessageRequest cannot be null.";
			LOG.error(errorMessage);
			throw SdkClientException.create(errorMessage);
		}

//		sendMessageRequest.getRequestClientOptions().appendUserAgent(SQSExtendedClientConstants.USER_AGENT_HEADER);

		if (!clientConfiguration.isLargePayloadSupportEnabled()) {
			return super.sendMessage(sendMessageRequest);
		}

		if (sendMessageRequest.messageBody() == null || "".equals(sendMessageRequest.messageBody())) {
			String errorMessage = "messageBody cannot be null or empty.";
			LOG.error(errorMessage);
			throw SdkClientException.create(errorMessage);
		}

		if (clientConfiguration.isAlwaysThroughS3() || isLarge(sendMessageRequest)) {
			sendMessageRequest = storeMessageInS3(sendMessageRequest);
		}

		return super.sendMessage(sendMessageRequest);
	}

	/**
	 * <p>
	 * Retrieves one or more messages, with a maximum limit of 10 messages, from the
	 * specified queue. Downloads the message payloads from Amazon S3 when
	 * necessary. Long poll support is enabled by using the
	 * <code>WaitTimeSeconds</code> parameter. For more information, see <a href=
	 * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html"
	 * > Amazon SQS Long Poll </a> in the <i>Amazon SQS Developer Guide</i> .
	 * </p>
	 * <p>
	 * Short poll is the default behavior where a weighted random set of machines is
	 * sampled on a <code>ReceiveMessage</code> call. This means only the messages
	 * on the sampled machines are returned. If the number of messages in the queue
	 * is small (less than 1000), it is likely you will get fewer messages than you
	 * requested per <code>ReceiveMessage</code> call. If the number of messages in
	 * the queue is extremely small, you might not receive any messages in a
	 * particular <code>ReceiveMessage</code> response; in which case you should
	 * repeat the request.
	 * </p>
	 * <p>
	 * For each message returned, the response includes the following:
	 * </p>
	 *
	 * <ul>
	 * <li>
	 * <p>
	 * Message body
	 * </p>
	 * </li>
	 * <li>
	 * <p>
	 * MD5 digest of the message body. For information about MD5, go to
	 * <a href="http://www.faqs.org/rfcs/rfc1321.html">
	 * http://www.faqs.org/rfcs/rfc1321.html </a> .
	 * </p>
	 * </li>
	 * <li>
	 * <p>
	 * Message ID you received when you sent the message to the queue.
	 * </p>
	 * </li>
	 * <li>
	 * <p>
	 * Receipt handle.
	 * </p>
	 * </li>
	 * <li>
	 * <p>
	 * Message attributes.
	 * </p>
	 * </li>
	 * <li>
	 * <p>
	 * MD5 digest of the message attributes.
	 * </p>
	 * </li>
	 *
	 * </ul>
	 * <p>
	 * The receipt handle is the identifier you must provide when deleting the
	 * message. For more information, see <a href=
	 * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/ImportantIdentifiers.html"
	 * > Queue and Message Identifiers </a> in the <i>Amazon SQS Developer Guide</i>
	 * .
	 * </p>
	 * <p>
	 * You can provide the <code>VisibilityTimeout</code> parameter in your request,
	 * which will be applied to the messages that Amazon SQS returns in the
	 * response. If you do not include the parameter, the overall visibility timeout
	 * for the queue is used for the returned messages. For more information, see
	 * <a href=
	 * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html"
	 * > Visibility Timeout </a> in the <i>Amazon SQS Developer Guide</i> .
	 * </p>
	 * <p>
	 * <b>NOTE:</b> Going forward, new attributes might be added. If you are writing
	 * code that calls this action, we recommend that you structure your code so
	 * that it can handle new attributes gracefully.
	 * </p>
	 *
	 * @param receiveMessageRequest Container for the necessary parameters to
	 *                              execute the ReceiveMessage service method on
	 *                              AmazonSQS.
	 *
	 * @return The response from the ReceiveMessage service method, as returned by
	 *         AmazonSQS.
	 *
	 * @throws OverLimitException
	 *
	 * @throws AmazonClientException  If any internal errors are encountered inside
	 *                                the client while attempting to make the
	 *                                request or handle the response. For example if
	 *                                a network connection is not available.
	 * @throws AmazonServiceException If an error response is returned by AmazonSQS
	 *                                indicating either a problem with the data in
	 *                                the request, or a server side issue.
	 */
	public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest receiveMessageRequest) {

		if (receiveMessageRequest == null) {
			String errorMessage = "receiveMessageRequest cannot be null.";
			LOG.error(errorMessage);
			throw SdkClientException.create(errorMessage);
		}

//		receiveMessageRequest.getRequestClientOptions().appendUserAgent(SQSExtendedClientConstants.USER_AGENT_HEADER);

		if (!clientConfiguration.isLargePayloadSupportEnabled()) {
			return super.receiveMessage(receiveMessageRequest);
		}

		if (!receiveMessageRequest
				.messageAttributeNames()
				.contains(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME)
		) {
			receiveMessageRequest
			.messageAttributeNames()
			.add(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME);
		}

		ReceiveMessageResponse receiveMessageResult = super.receiveMessage(receiveMessageRequest);

		List<Message> messages = receiveMessageResult.messages();
		for (Message message : messages) {

			// for each received message check if they are stored in S3.
			MessageAttributeValue largePayloadAttributeValue = message.messageAttributes()
					.get(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME);
			if (largePayloadAttributeValue != null) {
				String messageBody = message.body();

				// read the S3 pointer from the message body JSON string.
				MessageS3Pointer s3Pointer = readMessageS3PointerFromJSON(messageBody);

				String s3MsgBucketName = s3Pointer.getS3BucketName();
				String s3MsgKey = s3Pointer.getS3Key();

				String origMsgBody = getTextFromS3(s3MsgBucketName, s3MsgKey);
				LOG.info("S3 object read, Bucket name: " + s3MsgBucketName + ", Object key: " + s3MsgKey + ".");

				message.setBody(origMsgBody);

				// remove the additional attribute before returning the message
				// to user.
				message.messageAttributes().remove(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME);

				// Embed s3 object pointer in the receipt handle.
				String modifiedReceiptHandle = embedS3PointerInReceiptHandle(message.getReceiptHandle(),
						s3MsgBucketName, s3MsgKey);

				message.setReceiptHandle(modifiedReceiptHandle);
			}
		}
		
		return receiveMessageResult;
	}

	/**
	 * <p>
	 * Deletes the specified message from the specified queue and deletes the
	 * message payload from Amazon S3 when necessary. You specify the message by
	 * using the message's <code>receipt handle</code> and not the
	 * <code>message ID</code> you received when you sent the message. Even if the
	 * message is locked by another reader due to the visibility timeout setting, it
	 * is still deleted from the queue. If you leave a message in the queue for
	 * longer than the queue's configured retention period, Amazon SQS automatically
	 * deletes it.
	 * </p>
	 * <p>
	 * <b>NOTE:</b> The receipt handle is associated with a specific instance of
	 * receiving the message. If you receive a message more than once, the receipt
	 * handle you get each time you receive the message is different. When you
	 * request DeleteMessage, if you don't provide the most recently received
	 * receipt handle for the message, the request will still succeed, but the
	 * message might not be deleted.
	 * </p>
	 * <p>
	 * <b>IMPORTANT:</b> It is possible you will receive a message even after you
	 * have deleted it. This might happen on rare occasions if one of the servers
	 * storing a copy of the message is unavailable when you request to delete the
	 * message. The copy remains on the server and might be returned to you again on
	 * a subsequent receive request. You should create your system to be idempotent
	 * so that receiving a particular message more than once is not a problem.
	 * </p>
	 *
	 * @param deleteMessageRequest Container for the necessary parameters to execute
	 *                             the DeleteMessage service method on AmazonSQS.
	 *
	 * @return The response from the DeleteMessage service method, as returned by
	 *         AmazonSQS.
	 *
	 * @throws ReceiptHandleIsInvalidException
	 * @throws InvalidIdFormatException
	 *
	 * @throws AmazonClientException           If any internal errors are
	 *                                         encountered inside the client while
	 *                                         attempting to make the request or
	 *                                         handle the response. For example if a
	 *                                         network connection is not available.
	 * @throws AmazonServiceException          If an error response is returned by
	 *                                         AmazonSQS indicating either a problem
	 *                                         with the data in the request, or a
	 *                                         server side issue.
	 */
	public DeleteMessageResponse deleteMessage(DeleteMessageRequest deleteMessageRequest) {

		if (deleteMessageRequest == null) {
			String errorMessage = "deleteMessageRequest cannot be null.";
			LOG.error(errorMessage);
			throw SdkClientException.create(errorMessage);
		}

		//deleteMessageRequest.getRequestClientOptions().appendUserAgent(SQSExtendedClientConstants.USER_AGENT_HEADER);

		if (!clientConfiguration.isLargePayloadSupportEnabled()) {
			return super.deleteMessage(deleteMessageRequest);
		}

		String receiptHandle = deleteMessageRequest.receiptHandle();
		String origReceiptHandle = receiptHandle;
		if (isS3ReceiptHandle(receiptHandle)) {
			deleteMessagePayloadFromS3(receiptHandle);
			origReceiptHandle = getOrigReceiptHandle(receiptHandle);
		}
		
		deleteMessageRequest.setReceiptHandle(origReceiptHandle);
		
		return super.deleteMessage(deleteMessageRequest);
	}

	/**
	 * <p>
	 * Delivers up to ten messages to the specified queue. This is a batch version
	 * of SendMessage. The result of the send action on each message is reported
	 * individually in the response. Uploads message payloads to Amazon S3 when
	 * necessary.
	 * </p>
	 * <p>
	 * If the <code>DelaySeconds</code> parameter is not specified for an entry, the
	 * default for the queue is used.
	 * </p>
	 * <p>
	 * <b>IMPORTANT:</b>The following list shows the characters (in Unicode) that
	 * are allowed in your message, according to the W3C XML specification. For more
	 * information, go to http://www.faqs.org/rfcs/rfc1321.html. If you send any
	 * characters that are not included in the list, your request will be rejected.
	 * #x9 | #xA | #xD | [#x20 to #xD7FF] | [#xE000 to #xFFFD] | [#x10000 to
	 * #x10FFFF]
	 * </p>
	 * <p>
	 * <b>IMPORTANT:</b> Because the batch request can result in a combination of
	 * successful and unsuccessful actions, you should check for batch errors even
	 * when the call returns an HTTP status code of 200.
	 * </p>
	 * <b>IMPORTANT:</b> The input object may be modified by the method.
	 * </p>
	 * <p>
	 * <b>NOTE:</b>Some API actions take lists of parameters. These lists are
	 * specified using the param.n notation. Values of n are integers starting from
	 * 1. For example, a parameter list with two elements looks like this:
	 * </p>
	 * <p>
	 * <code>&Attribute.1=this</code>
	 * </p>
	 * <p>
	 * <code>&Attribute.2=that</code>
	 * </p>
	 *
	 * @param sendMessageBatchRequest Container for the necessary parameters to
	 *                                execute the SendMessageBatch service method on
	 *                                AmazonSQS.
	 *
	 * @return The response from the SendMessageBatch service method, as returned by
	 *         AmazonSQS.
	 *
	 * @throws BatchEntryIdsNotDistinctException
	 * @throws TooManyEntriesInBatchRequestException
	 * @throws BatchRequestTooLongException
	 * @throws UnsupportedOperationException
	 * @throws InvalidBatchEntryIdException
	 * @throws EmptyBatchRequestException
	 *
	 * @throws AmazonClientException                 If any internal errors are
	 *                                               encountered inside the client
	 *                                               while attempting to make the
	 *                                               request or handle the response.
	 *                                               For example if a network
	 *                                               connection is not available.
	 * @throws AmazonServiceException                If an error response is
	 *                                               returned by AmazonSQS
	 *                                               indicating either a problem
	 *                                               with the data in the request,
	 *                                               or a server side issue.
	 */
	public SendMessageBatchResponse sendMessageBatch(SendMessageBatchRequest sendMessageBatchRequest) {

		if (sendMessageBatchRequest == null) {
			String errorMessage = "sendMessageBatchRequest cannot be null.";
			LOG.error(errorMessage);
			throw SdkClientException.create(errorMessage);
		}

		//sendMessageBatchRequest.getRequestClientOptions().appendUserAgent(SQSExtendedClientConstants.USER_AGENT_HEADER);

		if (!clientConfiguration.isLargePayloadSupportEnabled()) {
			return super.sendMessageBatch(sendMessageBatchRequest);
		}

		List<SendMessageBatchRequestEntry> batchEntries = sendMessageBatchRequest.entries();

		int index = 0;
		for (SendMessageBatchRequestEntry entry : batchEntries) {
			if (clientConfiguration.isAlwaysThroughS3() || isLarge(entry)) {
				batchEntries.set(index, storeMessageInS3(entry));
			}
			++index;
		}

		return super.sendMessageBatch(sendMessageBatchRequest);
	}

	/**
	 * <p>
	 * Deletes up to ten messages from the specified queue. This is a batch version
	 * of DeleteMessage. The result of the delete action on each message is reported
	 * individually in the response. Also deletes the message payloads from Amazon
	 * S3 when necessary.
	 * </p>
	 * <p>
	 * <b>IMPORTANT:</b> Because the batch request can result in a combination of
	 * successful and unsuccessful actions, you should check for batch errors even
	 * when the call returns an HTTP status code of 200.
	 * </p>
	 * <p>
	 * <b>NOTE:</b>Some API actions take lists of parameters. These lists are
	 * specified using the param.n notation. Values of n are integers starting from
	 * 1. For example, a parameter list with two elements looks like this:
	 * </p>
	 * <p>
	 * <code>&Attribute.1=this</code>
	 * </p>
	 * <p>
	 * <code>&Attribute.2=that</code>
	 * </p>
	 *
	 * @param deleteMessageBatchRequest Container for the necessary parameters to
	 *                                  execute the DeleteMessageBatch service
	 *                                  method on AmazonSQS.
	 *
	 * @return The response from the DeleteMessageBatch service method, as returned
	 *         by AmazonSQS.
	 *
	 * @throws BatchEntryIdsNotDistinctException
	 * @throws TooManyEntriesInBatchRequestException
	 * @throws InvalidBatchEntryIdException
	 * @throws EmptyBatchRequestException
	 *
	 * @throws AmazonClientException                 If any internal errors are
	 *                                               encountered inside the client
	 *                                               while attempting to make the
	 *                                               request or handle the response.
	 *                                               For example if a network
	 *                                               connection is not available.
	 * @throws AmazonServiceException                If an error response is
	 *                                               returned by AmazonSQS
	 *                                               indicating either a problem
	 *                                               with the data in the request,
	 *                                               or a server side issue.
	 */
	public DeleteMessageBatchResponse deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest) {

		if (deleteMessageBatchRequest == null) {
			String errorMessage = "deleteMessageBatchRequest cannot be null.";
			LOG.error(errorMessage);
			throw SdkClientException.create(errorMessage);
		}

//		deleteMessageBatchRequest.getRequestClientOptions()
//				.appendUserAgent(SQSExtendedClientConstants.USER_AGENT_HEADER);

		if (!clientConfiguration.isLargePayloadSupportEnabled()) {
			return super.deleteMessageBatch(deleteMessageBatchRequest);
		}

		for (DeleteMessageBatchRequestEntry entry : deleteMessageBatchRequest.entries()) {
			String receiptHandle = entry.receiptHandle();
			String origReceiptHandle = receiptHandle;
			if (isS3ReceiptHandle(receiptHandle)) {
				deleteMessagePayloadFromS3(receiptHandle);
				origReceiptHandle = getOrigReceiptHandle(receiptHandle);
			}
			
			entry.setReceiptHandle(origReceiptHandle);
		}
		
		return super.deleteMessageBatch(deleteMessageBatchRequest);
	}

	private void deleteMessagePayloadFromS3(String receiptHandle) {
		String s3MsgBucketName = getFromReceiptHandleByMarker(receiptHandle,
				SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER);
		String s3MsgKey = getFromReceiptHandleByMarker(receiptHandle, SQSExtendedClientConstants.S3_KEY_MARKER);
		try {
			clientConfiguration.getAmazonS3Client().deleteObject(s3MsgBucketName, s3MsgKey);
		} catch (SdkServiceException e) {
			String errorMessage = "Failed to delete the S3 object which contains the SQS message payload. SQS message was not deleted.";
			LOG.error(errorMessage, e);
			//throw SdkServiceException.create(errorMessage, e);
			throw SdkClientException.create(errorMessage, e);
		} catch (SdkClientException e) {
			String errorMessage = "Failed to delete the S3 object which contains the SQS message payload. SQS message was not deleted.";
			LOG.error(errorMessage, e);
			throw SdkClientException.create(errorMessage, e);
		}
		
		LOG.info("S3 object deleted, Bucket name: " + s3MsgBucketName + ", Object key: " + s3MsgKey + ".");
	}

	private void checkMessageAttributes(Map<String, MessageAttributeValue> messageAttributes) {
		int msgAttributesSize = getMsgAttributesSize(messageAttributes);
		if (msgAttributesSize > clientConfiguration.getMessageSizeThreshold()) {
			String errorMessage = "Total size of Message attributes is " + msgAttributesSize
					+ " bytes which is larger than the threshold of " + clientConfiguration.getMessageSizeThreshold()
					+ " Bytes. Consider including the payload in the message body instead of message attributes.";
			LOG.error(errorMessage);
			throw SdkClientException.create(errorMessage);
		}

		int messageAttributesNum = messageAttributes.size();
		if (messageAttributesNum > SQSExtendedClientConstants.MAX_ALLOWED_ATTRIBUTES) {
			String errorMessage = "Number of message attributes [" + messageAttributesNum
					+ "] exceeds the maximum allowed for large-payload messages ["
					+ SQSExtendedClientConstants.MAX_ALLOWED_ATTRIBUTES + "].";
			LOG.error(errorMessage);
			throw SdkClientException.create(errorMessage);
		}

		MessageAttributeValue largePayloadAttributeValue = messageAttributes
				.get(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME);
		if (largePayloadAttributeValue != null) {
			String errorMessage = "Message attribute name " + SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME
					+ " is reserved for use by SQS extended client.";
			LOG.error(errorMessage);
			throw SdkClientException.create(errorMessage);
		}
	}

	private String embedS3PointerInReceiptHandle(
			String receiptHandle, 
			String s3MsgBucketName,
			String s3MsgKey
	) {
		String modifiedReceiptHandle = SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER + s3MsgBucketName
				+ SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER + SQSExtendedClientConstants.S3_KEY_MARKER + s3MsgKey
				+ SQSExtendedClientConstants.S3_KEY_MARKER + receiptHandle;
		return modifiedReceiptHandle;
	}

	private MessageS3Pointer readMessageS3PointerFromJSON(String messageBody) {

		MessageS3Pointer s3Pointer = null;
		try {
			JsonDataConverter jsonDataConverter = new JsonDataConverter();
			s3Pointer = jsonDataConverter.deserializeFromJson(messageBody, MessageS3Pointer.class);
		} catch (Exception e) {
			String errorMessage = "Failed to read the S3 object pointer from an SQS message. Message was not received.";
			LOG.error(errorMessage, e);
			throw SdkClientException.create(errorMessage, e);
		}
		return s3Pointer;
	}

	private String getOrigReceiptHandle(String receiptHandle) {
		int secondOccurence = receiptHandle.indexOf(SQSExtendedClientConstants.S3_KEY_MARKER,
				receiptHandle.indexOf(SQSExtendedClientConstants.S3_KEY_MARKER) + 1);
		return receiptHandle.substring(secondOccurence + SQSExtendedClientConstants.S3_KEY_MARKER.length());
	}

	private String getFromReceiptHandleByMarker(String receiptHandle, String marker) {
		int firstOccurence = receiptHandle.indexOf(marker);
		int secondOccurence = receiptHandle.indexOf(marker, firstOccurence + 1);
		return receiptHandle.substring(firstOccurence + marker.length(), secondOccurence);
	}

	private boolean isS3ReceiptHandle(String receiptHandle) {
		return receiptHandle.contains(SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER)
				&& receiptHandle.contains(SQSExtendedClientConstants.S3_KEY_MARKER);
	}

	private String getTextFromS3(String s3BucketName, String s3Key) {
		GetObjectRequest getObjectRequest = new GetObjectRequest(s3BucketName, s3Key);
		String embeddedText = null;
		S3Object obj = null;
		try {
			obj = clientConfiguration.getAmazonS3Client().getObject(getObjectRequest);
		} catch (SdkServiceException e) {
			String errorMessage = "Failed to get the S3 object which contains the message payload. Message was not received.";
			LOG.error(errorMessage, e);
			throw SdkClientException.create(errorMessage, e);
		} catch (SdkClientException e) {
			String errorMessage = "Failed to get the S3 object which contains the message payload. Message was not received.";
			LOG.error(errorMessage, e);
			throw SdkClientException.create(errorMessage, e);
		}
		S3ObjectInputStream is = obj.getObjectContent();
		try {
			embeddedText = IOUtils.toString(is);
		} catch (IOException e) {
			String errorMessage = "Failure when handling the message which was read from S3 object. Message was not received.";
			LOG.error(errorMessage, e);
			throw SdkClientException.create(errorMessage, e);
		} finally {
			IOUtils.closeQuietly(is, LOG);
		}
		return embeddedText;
	}

	private boolean isLarge(SendMessageRequest sendMessageRequest) {
		int msgAttributesSize = getMsgAttributesSize(sendMessageRequest.getMessageAttributes());
		long msgBodySize = getStringSizeInBytes(sendMessageRequest.getMessageBody());
		long totalMsgSize = msgAttributesSize + msgBodySize;
		return (totalMsgSize > clientConfiguration.getMessageSizeThreshold());
	}

	private boolean isLarge(SendMessageBatchRequestEntry batchEntry) {
		int msgAttributesSize = getMsgAttributesSize(batchEntry.getMessageAttributes());
		long msgBodySize = getStringSizeInBytes(batchEntry.getMessageBody());
		long totalMsgSize = msgAttributesSize + msgBodySize;
		return (totalMsgSize > clientConfiguration.getMessageSizeThreshold());
	}

	private int getMsgAttributesSize(Map<String, MessageAttributeValue> msgAttributes) {
		int totalMsgAttributesSize = 0;
		for (Entry<String, MessageAttributeValue> entry : msgAttributes.entrySet()) {
			totalMsgAttributesSize += getStringSizeInBytes(entry.getKey());

			MessageAttributeValue entryVal = entry.getValue();
			if (entryVal.getDataType() != null) {
				totalMsgAttributesSize += getStringSizeInBytes(entryVal.getDataType());
			}

			String stringVal = entryVal.getStringValue();
			if (stringVal != null) {
				totalMsgAttributesSize += getStringSizeInBytes(entryVal.getStringValue());
			}

			ByteBuffer binaryVal = entryVal.getBinaryValue();
			if (binaryVal != null) {
				totalMsgAttributesSize += binaryVal.array().length;
			}
		}
		return totalMsgAttributesSize;
	}

	private SendMessageBatchRequestEntry storeMessageInS3(SendMessageBatchRequestEntry batchEntry) {

		checkMessageAttributes(batchEntry.getMessageAttributes());

		String s3Key = UUID.randomUUID().toString();

		// Read the content of the message from message body
		String messageContentStr = batchEntry.getMessageBody();

		Long messageContentSize = getStringSizeInBytes(messageContentStr);

		// Add a new message attribute as a flag
		MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
		messageAttributeValue.setDataType("Number");
		messageAttributeValue.setStringValue(messageContentSize.toString());
		batchEntry.addMessageAttributesEntry(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME, messageAttributeValue);

		// Store the message content in S3.
		storeTextInS3(s3Key, messageContentStr, messageContentSize);

		LOG.info("S3 object created, Bucket name: " + clientConfiguration.getS3BucketName() + ", Object key: " + s3Key
				+ ".");

		// Convert S3 pointer (bucket name, key, etc) to JSON string
		MessageS3Pointer s3Pointer = new MessageS3Pointer(clientConfiguration.getS3BucketName(), s3Key);
		String s3PointerStr = getJSONFromS3Pointer(s3Pointer);

		// Storing S3 pointer in the message body.
		batchEntry.setMessageBody(s3PointerStr);

		return batchEntry;
	}

	private SendMessageRequest storeMessageInS3(SendMessageRequest sendMessageRequest) {

		checkMessageAttributes(sendMessageRequest.messageAttributes());

		String s3Key = UUID.randomUUID().toString();

		// Read the content of the message from message body
		String messageContentStr = sendMessageRequest.messageBody();

		Long messageContentSize = getStringSizeInBytes(messageContentStr);

		// Add a new message attribute as a flag
		MessageAttributeValue messageAttributeValue = MessageAttributeValue
			.builder()
			.dataType("Number")
			.stringValue(messageContentSize.toString())
			.build();
		sendMessageRequest
			.messageAttributes()
			.put(
				SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME,
				messageAttributeValue
			);

		// Store the message content in S3.
		storeTextInS3(s3Key, messageContentStr, messageContentSize);
		LOG.info("S3 object created, Bucket name: " + clientConfiguration.getS3BucketName() + ", Object key: " + s3Key + ".");

		// Convert S3 pointer (bucket name, key, etc) to JSON string
		MessageS3Pointer s3Pointer = new MessageS3Pointer(clientConfiguration.getS3BucketName(), s3Key);

		String s3PointerStr = getJSONFromS3Pointer(s3Pointer);

		// Storing S3 pointer in the message body.
		sendMessageRequest.setMessageBody(s3PointerStr);

		return sendMessageRequest;
	}

	private String getJSONFromS3Pointer(MessageS3Pointer s3Pointer) {
		try {
			JsonDataConverter jsonDataConverter = new JsonDataConverter();
			String s3PointerStr = jsonDataConverter.serializeToJson(s3Pointer);
			return s3PointerStr;
		} catch (Exception e) {
			String errorMessage = "Failed to convert S3 object pointer to text. Message was not sent.";
			LOG.error(errorMessage, e);
			throw SdkClientException.create(errorMessage, e);
		}
	}

	private void storeTextInS3(
		String s3Key, 
		String messageContentStr, 
		Long messageContentSize
	) {
		try {
			PutObjectRequest putObjectRequest = PutObjectRequest
					.builder()
					.contentLength(messageContentSize)
					.bucket(clientConfiguration.getS3BucketName())
					.key(s3Key)
					.build();
			RequestBody requestBody = RequestBody
					.fromString(messageContentStr);
			clientConfiguration
					.getAmazonS3Client()
					.putObject(putObjectRequest, requestBody);
		}
		catch (SdkServiceException e) {
			String errorMessage = "Failed to store the message content in an S3 object. SQS message was not sent.";
			LOG.error(errorMessage, e);
			throw SdkClientException.create(errorMessage, e);
		}
		catch (SdkClientException e) {
			String errorMessage = "Failed to store the message content in an S3 object. SQS message was not sent.";
			LOG.error(errorMessage, e);
			throw SdkClientException.create(errorMessage, e);
		}
	}

	private static long getStringSizeInBytes(String str) {
		try (
			CountingOutputStream counterOutputStream = new CountingOutputStream();
			Writer writer = new OutputStreamWriter(counterOutputStream, "UTF-8");
		) {
			writer.write(str);			
			return counterOutputStream.getTotalSize();
		} catch (IOException e) {
			String errorMessage = "Failed to calculate the size of message payload.";
			LOG.error(errorMessage, e);
			throw SdkClientException.create(errorMessage, e);
		}
	}
}
