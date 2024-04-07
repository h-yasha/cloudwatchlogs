import {
	CloudWatchLogsClient,
	CreateLogGroupCommand,
	CreateLogStreamCommand,
	DescribeLogStreamsCommand,
	PutLogEventsCommand,
	type CloudWatchLogsClientConfig,
	type InputLogEvent,
	type InvalidSequenceTokenException,
	type ResourceAlreadyExistsException,
} from "@aws-sdk/client-cloudwatch-logs";
import pThrottle from "p-throttle";

interface Log extends InputLogEvent {
	timestamp: number;
	message: string;
}

export class CloudWatchLogs {
	private static isInvalidSequenceTokenException(
		err: unknown,
	): err is InvalidSequenceTokenException {
		if (err instanceof Error) {
			return err.name === "InvalidSequenceTokenException";
		}

		return false;
	}

	private static isResourceAlreadyExistsException(
		err: unknown,
	): err is ResourceAlreadyExistsException {
		if (err instanceof Error) {
			return err.name === "ResourceAlreadyExistsException";
		}

		return false;
	}

	//

	private static client: CloudWatchLogsClient | undefined;
	private static throttle: ReturnType<typeof pThrottle> | undefined;

	static initialize(
		config: CloudWatchLogsClientConfig & {
			/** @default 1000 */
			flushInterval: number;
		},
	) {
		CloudWatchLogs.client = new CloudWatchLogsClient(config);

		CloudWatchLogs.flushInterval = config.flushInterval ?? 1000;

		CloudWatchLogs.throttle = pThrottle({
			interval: 1000,
			limit: 1,
		});
	}

	//----------------------------------------------------------------------------

	private static flushInterval = 1_000;
	private static lastFlush = Date.now();
	private static sequenceToken: string | undefined = undefined;

	// --------------------------------------------------------------------------
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
	private static readonly MAX_EVENT_SIZE = 2 ** 10 * 256; // 256 Kb

	// https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
	private static readonly MAX_BUFFER_LENGTH = 10_000;
	private static readonly MAX_BUFFER_SIZE = 1_048_576;

	private static bufferedLogs = new Map<`${string}{::}${string}`, Log[]>();
	private static pushLog(
		logGroupName: string,
		logStreamName: string,
		log: Log,
	) {
		const key = `${logGroupName}{::}${logStreamName}` as const;
		if (!CloudWatchLogs.bufferedLogs.has(key)) {
			CloudWatchLogs.bufferedLogs.set(key, []);
		}

		CloudWatchLogs.bufferedLogs.get(key)?.push(log);
	}

	private static reachedNumberOfLogsLimit(): boolean {
		let length = 0;
		for (const logs of CloudWatchLogs.bufferedLogs.values()) {
			length += logs.length;
		}

		return length === CloudWatchLogs.MAX_BUFFER_LENGTH;
	}

	private static reachedBufferSizeLimit(newLog: Log): boolean {
		let currentSize = 0;
		for (const logs of CloudWatchLogs.bufferedLogs.values()) {
			currentSize += logs.reduce(
				(acc, curr) => acc + curr.message.length + 26,
				0,
			);
		}

		return (
			currentSize + newLog.message.length + 26 >= CloudWatchLogs.MAX_BUFFER_SIZE
		);
	}

	private static logEventExceedsSize(log: Log): boolean {
		return log.message.length >= CloudWatchLogs.MAX_EVENT_SIZE;
	}

	private static orderLogs(): void {
		for (const logs of CloudWatchLogs.bufferedLogs.values()) {
			logs.sort((a, b) => a.timestamp - b.timestamp);
		}
	}

	private static shouldDoAPeriodicFlush() {
		const now = Date.now();
		const timeSinceLastFlush = now - CloudWatchLogs.lastFlush;

		return timeSinceLastFlush > CloudWatchLogs.flushInterval;
	}

	private static async addErrorLog(
		groupName: string,
		streamName: string,
		errorLog: { message: string; error: string },
	) {
		CloudWatchLogs.pushLog(groupName, streamName, {
			timestamp: Date.now(),
			message: JSON.stringify(errorLog),
		});

		await CloudWatchLogs.flush();
	}

	private static wipeLogs(): void {
		CloudWatchLogs.bufferedLogs.clear();
	}

	//

	/**
	 * @throws {Error} if CloudWatchLogs client is not initialized.
	 */
	private static async createLogGroup(logGroupName: string) {
		if (!CloudWatchLogs.client) {
			throw new Error("CloudWatchLogs client is not initialized.");
		}

		try {
			await CloudWatchLogs.client.send(
				new CreateLogGroupCommand({ logGroupName }),
			);
		} catch (error: unknown) {
			if (CloudWatchLogs.isResourceAlreadyExistsException(error)) {
				return;
			}
			throw new Error("Create Log Group Failed", { cause: error });
		}
	}

	/**
	 * @throws {Error} if CloudWatchLogs client is not initialized.
	 */
	private static async createLogStream(
		logGroupName: string,
		logStreamName: string,
	) {
		if (!CloudWatchLogs.client) {
			throw new Error("CloudWatchLogs client is not initialized.");
		}

		try {
			await CloudWatchLogs.client.send(
				new CreateLogStreamCommand({
					logGroupName,
					logStreamName,
				}),
			);
		} catch (error: unknown) {
			if (CloudWatchLogs.isResourceAlreadyExistsException(error)) {
				return;
			}

			throw new Error("Create Log Stream Failed", { cause: error });
		}
	}

	/**
	 * @throws {Error} if CloudWatchLogs client is not initialized.
	 */
	private static async nextToken(logGroupName: string, logStreamName: string) {
		if (!CloudWatchLogs.client) {
			throw new Error("CloudWatchLogs client is not initialized.");
		}

		const output = await CloudWatchLogs.client.send(
			new DescribeLogStreamsCommand({
				logGroupName,
				logStreamNamePrefix: logStreamName,
			}),
		);

		if (output.logStreams?.length === 0) {
			throw new Error("LogStream not found.");
		}

		CloudWatchLogs.sequenceToken = output.logStreams?.[0]?.uploadSequenceToken;
	}

	/**
	 * @throws {Error} if CloudWatchLogs client is not initialized.
	 */
	private static async putEventLogs(
		logGroupName: string,
		logStreamName: string,
		logEvents: Log[],
	) {
		if (logEvents.length === 0) {
			return;
		}

		if (!CloudWatchLogs.client) {
			throw new Error("CloudWatchLogs client is not initialized.");
		}

		try {
			const output = await CloudWatchLogs.client.send(
				new PutLogEventsCommand({
					logEvents,
					logGroupName,
					logStreamName,
					sequenceToken: CloudWatchLogs.sequenceToken,
				}),
			);

			CloudWatchLogs.sequenceToken = output.nextSequenceToken;
		} catch (error: unknown) {
			if (CloudWatchLogs.isInvalidSequenceTokenException(error)) {
				CloudWatchLogs.sequenceToken = error.expectedSequenceToken;
			} else {
				throw new Error("Put Log Events Failed", { cause: error });
			}
		}
	}

	/**
	 * @throws {Error} if CloudWatchLogs throttle is not initialized.
	 */
	static get flush() {
		if (!CloudWatchLogs.throttle) {
			throw new Error("CloudWatchLogs throttle is not initialized.");
		}

		return CloudWatchLogs.throttle(async () => {
			try {
				CloudWatchLogs.orderLogs();

				const entries = Array.from(CloudWatchLogs.bufferedLogs.entries());
				CloudWatchLogs.wipeLogs();

				for (const [key, logs] of entries) {
					const [groupName, streamName] = key.split("{::}") as [string, string];
					await CloudWatchLogs.putEventLogs(groupName, streamName, logs);
				}
			} catch (e: unknown) {
				await CloudWatchLogs.addErrorLog("cloudwatch_logger", "errors", {
					message: "flushing error",
					error: String(e),
				});
			} finally {
				CloudWatchLogs.lastFlush = Date.now();
			}
		});
	}

	//

	readonly logGroupName: string;
	readonly logStreamName: string;

	/**
	 * @throws {Error} if CloudWatchLogs client is not initialized.
	 */
	constructor({
		logGroupName,
		logStreamName,

		options,
	}: {
		logGroupName: string;
		logStreamName: string;

		options?: {
			/** @default "CloudWatcLogsErrors" */
			errorLogGroupName?: string;
		};
	}) {
		if (!CloudWatchLogs.client) {
			throw new Error("CloudWatchLogs client is not initialized.");
		}

		this.logGroupName = logGroupName;
		this.logStreamName = logStreamName;

		this.initialize(
			Object.assign(
				{
					errorLogGroupName: "CloudWatcLogsErrors",
				},
				options,
			),
		);
	}

	private async initialize(options: { errorLogGroupName: string }) {
		try {
			await CloudWatchLogs.createLogGroup(
				options.errorLogGroupName || "CloudWatchLogsError",
			);
			await CloudWatchLogs.createLogGroup(this.logGroupName);
			await CloudWatchLogs.createLogStream(
				this.logGroupName,
				this.logStreamName,
			);
			await CloudWatchLogs.nextToken(this.logGroupName, this.logStreamName);
		} catch (e) {
			await CloudWatchLogs.addErrorLog(this.logGroupName, this.logStreamName, {
				message: "initialization error",
				error: String(e),
			});
		}
	}

	addLog(log: Log): boolean {
		if (CloudWatchLogs.logEventExceedsSize(log)) {
			console.error("Log event exceeds size limit");
			return false;
		}

		if (!CloudWatchLogs.reachedBufferSizeLimit(log)) {
			CloudWatchLogs.pushLog(this.logGroupName, this.logStreamName, log);
			return (
				CloudWatchLogs.reachedNumberOfLogsLimit() ||
				CloudWatchLogs.shouldDoAPeriodicFlush()
			);
		}

		setImmediate(() => {
			this.addLog(log);
		});

		return true;
	}
}
