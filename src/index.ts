import {
	CloudWatchLogsClient,
	type CloudWatchLogsClientConfig,
	CreateLogGroupCommand,
	CreateLogStreamCommand,
	type InputLogEvent,
	InvalidParameterException,
	PutLogEventsCommand,
	ResourceAlreadyExistsException,
} from "@aws-sdk/client-cloudwatch-logs";

const DEBUG = !!process.env.DUCK_LOGGER_DEBUG;
function debug(message: string, ...optionalParams: unknown[]) {
	if (DEBUG) {
		console.debug(message, ...optionalParams);
	}
}

export interface Config extends CloudWatchLogsClientConfig {
	/**
	 * When set to 0 it will be disabled.
	 */
	flushInterval: number;
	/**
	 * The maximum max message size of cloudwatch is 256 Kb
	 */
	overSizedMessageAction: "clip" | "error" | "console";
}

export interface Log extends InputLogEvent {
	timestamp: number;
	message: string;
}

type OrderedLogs = Array<
	[streamGroup: string, streamName: string, logs: Log[]]
>;

// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
// https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
export const CLOUDWATCH_FIXED_EVENT_PREFIX = 26 /* byte */;
export const CLOUDWATCH_MAX_EVENT_SIZE = 2 ** 18 -
	CLOUDWATCH_FIXED_EVENT_PREFIX /* byte */; // 256 KB - 26 bytes.

export const CLOUDWTACH_MAX_BUFFER_LENGTH = 10_000;
export const CLOUDWATCH_MAX_BUFFER_SIZE = 2 ** 20 /* byte */; // 1 MB

export class CloudWatchLogger {
	private static isResourceAlreadyExistsException(
		error: unknown,
	): error is ResourceAlreadyExistsException {
		if (error instanceof ResourceAlreadyExistsException) {
			return true;
		}

		if (error instanceof Error) {
			return error.name === "ResourceAlreadyExistsException";
		}

		return false;
	}

	static logEventExceedsSize(log: Log): boolean {
		return Buffer.byteLength(log.message, "utf-8") >
			CLOUDWATCH_MAX_EVENT_SIZE;
	}

	//-------------------------------------------------------------------------

	readonly client: CloudWatchLogsClient;
	private readonly overSizedMessageAction: Config["overSizedMessageAction"];

	private _flushInterval = 0;
	private flushIntervalInterval: NodeJS.Timeout | undefined = undefined;
	private intervalCallback() {
		debug("flush interval");
		for (const logs of this.bufferedLogs.values()) {
			if (logs.length) {
				debug("flush interval: has logs");
				this.flush();
				break;
			}
		}
	}

	private get flushInterval() {
		return this._flushInterval;
	}

	private set flushInterval(value) {
		if (this.flushIntervalInterval) {
			clearInterval(this.flushIntervalInterval);
		}

		this._flushInterval = value;
		this.flushIntervalInterval = setInterval(
			this.intervalCallback.bind(this),
			this._flushInterval,
		);
	}

	private readonly logGroups = new Map<string, string[]>();
	private readonly bufferedLogs = new Map<`${string}{::}${string}`, Log[]>();

	constructor(config: Config) {
		this.client = new CloudWatchLogsClient(config);

		this.flushInterval = config.flushInterval;
		this.overSizedMessageAction = config.overSizedMessageAction;
	}

	private pushLog(
		logGroupName: string,
		logStreamName: string,
		log: Log,
	): void {
		const key = `${logGroupName}{::}${logStreamName}` as const;

		if (!this.bufferedLogs.has(key)) {
			this.bufferedLogs.set(key, []);
		}

		this.bufferedLogs.get(key)?.push(log);
	}

	private reachedBufferLimits(
		logGroup: string,
		logStream: string,
		newLog: Log,
	): boolean {
		const logs = this.bufferedLogs.get(`${logGroup}{::}${logStream}`);
		if (!logs) {
			debug(
				"[reachedBufferLimits]",
				false,
				`${logGroup}{::}${logStream}`,
				"`logStream` not in buffered logs",
			);

			return false;
		}

		if (logs.length + 1 >= CLOUDWTACH_MAX_BUFFER_LENGTH) {
			debug(
				"[reachedBufferLimits]",
				true,
				{ length: logs.length },
				"more than CLOUDWTACH_MAX_BUFFER_LENGTH",
			);

			return true;
		}

		const bufferSize = logs.reduce(
			(accumulator, log) =>
				accumulator +
				Buffer.byteLength(log.message, "utf-8") +
				CLOUDWATCH_FIXED_EVENT_PREFIX,
			0,
		);

		const messageSize = Buffer.byteLength(newLog.message, "utf-8") +
			CLOUDWATCH_FIXED_EVENT_PREFIX;

		const totalSize = bufferSize + messageSize;
		const result = totalSize >= CLOUDWATCH_MAX_BUFFER_SIZE;

		debug(
			"[reachedBufferLimits]",
			"bufferSize",
			bufferSize,
			"messageSize",
			messageSize,
			"totalSize",
			totalSize,
			"result",
			result,
		);

		return result;
	}

	private getOrderedLogs(): OrderedLogs {
		const all: OrderedLogs = [];

		for (const [key, value] of this.bufferedLogs.entries()) {
			const [streamGroup, streamName] = key.split("{::}") as [
				string,
				string,
			];

			this.bufferedLogs.set(key, []);

			all.push([
				streamGroup,
				streamName,
				value.sort((a, b) => a.timestamp - b.timestamp),
			]);
		}

		return all;
	}

	// ensure that logs have been _copied_ and cleared syncnorously
	flush(): Promise<void> {
		const entries = this.getOrderedLogs();
		// debug("flushing", { entries });
		return this._flush(entries);
	}

	private async _flush(entries: OrderedLogs): Promise<void> {
		debug("flusher");

		for (const [groupName, streamName, logs] of entries) {
			try {
				try {
					await this.putEventLogs(groupName, streamName, logs);
				} catch (error) {
					console.error(error);
					this.logError("cloudwatch_logger", "errors", {
						message: "flushing error (will retry)",
						error: String(error),
						_error: error,
						cause: String((error as Error).cause),
						groupName,
						streamName,
						logsCount: logs.length,
						logsRawTotalSize: logs.reduce(
							(acc, log) => acc + Buffer.byteLength(log.message),
							0,
						),
					});

					if (!logs.length) {
						continue;
					}

					const half = Math.ceil(logs.length / 2);

					while (logs.length) {
						const partial = logs.splice(0, half);

						if (error instanceof InvalidParameterException) {
							for (const record of partial) {
								record.message = Buffer.from(record.message)
									.subarray(0, CLOUDWATCH_MAX_EVENT_SIZE)
									.toString();
							}
						}

						await this.putEventLogs(groupName, streamName, partial);
					}
				}
			} catch (error) {
				console.error(error);
				this.logError("cloudwatch_logger", "errors", {
					message: "flushing error",
					error: String(error),
					_error: error,
					cause: String((error as Error).cause),
					stack: String((error as Error).stack),
				});
			}
		}
	}

	/**
	 * can return {@link Error} when {@link overSizedMessageAction} is set to `error`.
	 */
	log(
		logGroupName: string,
		logStreamName: string,
		log: Log,
	): Error | undefined {
		if (this.reachedBufferLimits(logGroupName, logStreamName, log)) {
			this.flush();
		}

		let log_ = log;

		if (CloudWatchLogger.logEventExceedsSize(log)) {
			switch (this.overSizedMessageAction) {
				case "clip": {
					const message = Buffer.from(log.message)
						.subarray(0, CLOUDWATCH_MAX_EVENT_SIZE)
						.toString();

					log_ = { timestamp: log.timestamp, message };

					break;
				}

				case "error": {
					return new Error("Log event exceeds size limit: 256Kb");
				}

				case "console": {
					try {
						console.info(JSON.parse(log.message));
					} catch {
						console.info(log.message);
					}

					return;
				}
			}
		}

		this.pushLog(logGroupName, logStreamName, log_);

		return;
	}

	private logError(
		groupName: string,
		streamName: string,
		log: { message: string; error: string } & Record<string, unknown>,
	): void {
		try {
			this.client.send(
				new PutLogEventsCommand({
					logGroupName: groupName,
					logStreamName: streamName,
					logEvents: [
						{
							timestamp: Date.now(),
							message: JSON.stringify(log),
						},
					],
				}),
			);
		} catch (error) {
			console.error(error);
			console.error(log);
		}
	}

	async createLogGroup(logGroupName: string): Promise<void> {
		if (this.logGroups.has(logGroupName)) {
			return;
		}

		try {
			const result = await this.client.send(
				new CreateLogGroupCommand({ logGroupName }),
			);
			debug("createLogGroup", { logGroupName }, result);

			this.logGroups.set(logGroupName, []);
		} catch (error) {
			if (CloudWatchLogger.isResourceAlreadyExistsException(error)) {
				debug("createLogGroup", { logGroupName }, error.message);

				this.logGroups.set(logGroupName, []);
				return;
			}

			throw new Error("Create Log Group Failed", { cause: error });
		}
	}

	async createLogStream(
		logGroupName: string,
		logStreamName: string,
	): Promise<void> {
		try {
			await this.createLogGroup(logGroupName);

			const logGroup = this.logGroups.get(logGroupName);
			if (logGroup?.includes(logStreamName)) {
				return;
			}

			const result = await this.client.send(
				new CreateLogStreamCommand({
					logGroupName,
					logStreamName,
				}),
			);
			debug("createLogStream", { logStreamName }, result);

			logGroup?.push(logStreamName);
		} catch (error) {
			if (CloudWatchLogger.isResourceAlreadyExistsException(error)) {
				debug("createLogStream", { logStreamName }, error.message);
				this.logGroups.get(logGroupName)?.push(logStreamName);
				return;
			}

			throw new Error("Create Log Stream Failed", { cause: error });
		}
	}

	async putEventLogs(
		logGroupName: string,
		logStreamName: string,
		logEvents: Log[],
	): Promise<void> {
		if (logEvents.length === 0) {
			return;
		}

		try {
			await this.createLogStream(logGroupName, logStreamName);

			const result = await this.client.send(
				new PutLogEventsCommand({
					logEvents,
					logGroupName,
					logStreamName,
				}),
			);
			debug("putEventLogs", { logGroupName, logStreamName }, result);

			if (result.$metadata.httpStatusCode !== 200) {
				const result = await this.client.send(
					new PutLogEventsCommand({
						logEvents,
						logGroupName,
						logStreamName,
					}),
				);

				debug("putEventLogs", { logGroupName, logStreamName }, result);
			}
		} catch (error) {
			throw new Error("Put Log Events Failed", { cause: error });
		}
	}
}
