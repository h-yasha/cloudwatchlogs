import {
	CloudWatchLogsClient,
	type CloudWatchLogsClientConfig,
	CreateLogGroupCommand,
	CreateLogStreamCommand,
	type InputLogEvent,
	PutLogEventsCommand,
	ResourceAlreadyExistsException,
} from "@aws-sdk/client-cloudwatch-logs";
import pThrottle from "p-throttle";

const DEBUG = !!process.env.DUCK_LOGGER_DEBUG;
function debug(message: string, ...optionalParams: unknown[]) {
	if (DEBUG) {
		console.debug(message, ...optionalParams);
	}
}

export interface Log extends InputLogEvent {
	timestamp: number;
	message: string;
}

// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
// https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
export const CLOUDWATCH_FIXED_EVENT_PREFIX = 26 /* byte */;
export const CLOUDWATCH_MAX_EVENT_SIZE =
	2 ** 18 - CLOUDWATCH_FIXED_EVENT_PREFIX /* byte */; // 256 KB - 26 bytes.

export const CLOUDWTACH_MAX_BUFFER_LENGTH = 10_000;
export const CLOUDWATCH_MAX_BUFFER_SIZE = 2 ** 20 /* byte */; // 1 MB

function isResourceAlreadyExistsException(
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

//

const throttle = pThrottle({
	interval: 50,
	limit: 1,
});

let client: CloudWatchLogsClient | undefined;

/**
 * When {true} will clip messages to fit into the maximum max message size of cloudwatch (256 Kb)
 *
 * When {false} will return error on {@link addLog}
 *
 * @default {true}
 **/
// biome-ignore lint/style/useConst: <explanation>
export let clipMessages = true;

// TODO: use Proxy?
export const flushInterval = new (class FlushInterval {
	#value = 1000;
	#interval: NodeJS.Timeout | undefined = undefined;

	get value() {
		return this.#value;
	}

	set value(value) {
		if (this.#interval) {
			clearInterval(this.#interval);
		}
		this.#interval = setInterval(this.#intervalCallback, value);

		this.#value = value;
	}

	#intervalCallback() {
		debug("flush interval");
		for (const logs of bufferedLogs.values()) {
			if (logs.length) {
				debug("flush interval: has logs");
				flush();
				break;
			}
		}
	}
})();

export function setup(
	config: CloudWatchLogsClientConfig & {
		/** @default 1000 */
		flushInterval: number;
	},
): void {
	if (client) {
		return;
	}
	client = new CloudWatchLogsClient(config);

	flushInterval.value = config.flushInterval ?? 1000;
}

// ----------------------------------------------------------------------------

const logGroups = new Map<string, string[]>();
const bufferedLogs = new Map<`${string}{::}${string}`, Log[]>();

//----------------------------------------------------------------------------

function pushLog(logGroupName: string, logStreamName: string, log: Log): void {
	const key = `${logGroupName}{::}${logStreamName}` as const;
	if (!bufferedLogs.has(key)) {
		bufferedLogs.set(key, []);
	}

	bufferedLogs.get(key)?.push(log);
}

function reachedBufferLimits(
	logGroup: string,
	logStream: string,
	newLog: Log,
): boolean {
	const logs = bufferedLogs.get(`${logGroup}{::}${logStream}`);
	if (!logs) {
		return false;
	}

	if (logs.length + 1 >= CLOUDWTACH_MAX_BUFFER_LENGTH) {
		return true;
	}

	const size = logs.reduce(
		(accumulator, log) =>
			accumulator +
			Buffer.byteLength(log.message, "utf-8") +
			CLOUDWATCH_FIXED_EVENT_PREFIX,
		0,
	);

	const messageSize =
		Buffer.byteLength(newLog.message, "utf-8") + CLOUDWATCH_FIXED_EVENT_PREFIX;

	return size + messageSize >= CLOUDWATCH_MAX_BUFFER_SIZE;
}

function logEventExceedsSize(log: Log): boolean {
	return Buffer.byteLength(log.message, "utf-8") >= CLOUDWATCH_MAX_EVENT_SIZE;
}

function getOrderedLogs(): Array<
	[streamGroup: string, streamName: string, logs: Log[]]
> {
	const all: Array<[streamGroup: string, streamName: string, logs: Log[]]> = [];

	for (const [key, value] of bufferedLogs.entries()) {
		const [streamGroup, streamName] = key.split("{::}") as [string, string];

		bufferedLogs.set(key, []);

		all.push([
			streamGroup,
			streamName,
			value.sort((a, b) => a.timestamp - b.timestamp),
		]);
	}

	return all;
}

// TODO: instantly log out
async function addErrorLog(
	groupName: string,
	streamName: string,
	errorLog: { message: string; error: string } & Record<string, unknown>,
): Promise<void> {
	pushLog(groupName, streamName, {
		timestamp: Date.now(),
		message: JSON.stringify(errorLog),
	});

	await flush();
}

/**
 * @throws {Error} if CloudWatchLogs client is not initialized.
 */
async function createLogGroup(logGroupName: string): Promise<void> {
	if (!client) {
		throw new Error("CloudWatchLogs `client` is not initialized.");
	}

	if (logGroups.has(logGroupName)) {
		return;
	}

	try {
		const result = await client.send(
			new CreateLogGroupCommand({ logGroupName }),
		);
		debug("createLogGroup", { logGroupName }, result);

		logGroups.set(logGroupName, []);
	} catch (error) {
		if (isResourceAlreadyExistsException(error)) {
			debug("createLogGroup", { logGroupName }, error.message);

			logGroups.set(logGroupName, []);
			return;
		}

		throw new Error("Create Log Group Failed", { cause: error });
	}
}

/**
 * @throws {Error} if CloudWatchLogs client is not initialized.
 */
async function createLogStream(
	logGroupName: string,
	logStreamName: string,
): Promise<void> {
	if (!client) {
		throw new Error("CloudWatchLogs `client` is not initialized.");
	}

	try {
		await createLogGroup(logGroupName);

		const logGroup = logGroups.get(logGroupName);
		if (logGroup?.includes(logStreamName)) {
			return;
		}

		const result = await client.send(
			new CreateLogStreamCommand({
				logGroupName,
				logStreamName,
			}),
		);
		debug("createLogStream", { logStreamName }, result);

		logGroup?.push(logStreamName);
	} catch (error) {
		if (isResourceAlreadyExistsException(error)) {
			debug("createLogStream", { logStreamName }, error.message);
			logGroups.get(logGroupName)?.push(logStreamName);
			return;
		}

		throw new Error("Create Log Stream Failed", { cause: error });
	}
}

const putEventLogs = throttle(async function putEventLogs(
	logGroupName: string,
	logStreamName: string,
	logEvents: Log[],
): Promise<void> {
	if (logEvents.length === 0) {
		return;
	}

	if (!client) {
		throw new Error("CloudWatchLogs client is not initialized.");
	}

	try {
		await createLogStream(logGroupName, logStreamName);

		const result = await client.send(
			new PutLogEventsCommand({
				logEvents,
				logGroupName,
				logStreamName,
			}),
		);

		debug("putEventLogs", { logGroupName, logStreamName }, result);
	} catch (error) {
		throw new Error("Put Log Events Failed", { cause: error });
	}
});

// ensure that logs have been _copied_ and cleared syncnorously
export function flush(): Promise<void> {
	const entries = getOrderedLogs();

	debug("flushing", {
		entries,
	});

	return _flush(entries);
}

async function _flush(
	entries: ReturnType<typeof getOrderedLogs>,
): Promise<void> {
	debug("flusher");
	for (const [groupName, streamName, logs] of entries) {
		try {
			try {
				await putEventLogs(groupName, streamName, logs);
			} catch (error) {
				console.error(error);
				await addErrorLog("cloudwatch_logger", "errors", {
					message: "flushing error (will retry)",
					error: String(error),
					_error: error,
					cause: String((error as Error).cause),
					stack: String((error as Error).stack),
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

				const half = logs.length / 2;

				while (logs.length) {
					const partial = logs.splice(0, half);
					await putEventLogs(groupName, streamName, partial);
				}
			}
		} catch (error) {
			console.error(error);
			await addErrorLog("cloudwatch_logger", "errors", {
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
 * internal function used to add log messages to the buffer.
 *
 * can return {@link Error} when {@link clipMessages} is set to false.
 **/
export function addLog(
	logGroupName: string,
	logStreamName: string,
	log: Log,
): Error | undefined {
	if (reachedBufferLimits(logGroupName, logStreamName, log)) {
		flush();
	}

	let log_ = log;

	if (logEventExceedsSize(log)) {
		if (clipMessages) {
			const message = Buffer.from(log.message)
				.slice(0, CLOUDWATCH_MAX_EVENT_SIZE)
				.toString();

			log_ = { timestamp: log.timestamp, message };
		} else {
			return new Error("Log event exceeds size limit: 256Kb");
		}
	}

	setImmediate(pushLog, logGroupName, logStreamName, log_);

	return;
}

// ----------------------------------------------------------------------------

export type LogMessage = object | Record<string, unknown>;

export class Logger {
	readonly streamGroup: string;
	readonly streamName: string;
	// biome-ignore lint/suspicious/noExplicitAny: <explanation>
	readonly jsonReplacer?: (this: any, key: string, value: any) => any;

	constructor(
		streamGroup: string,
		streamName: string,
		// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		jsonReplacer?: (this: any, key: string, value: any) => any,
	) {
		this.streamGroup = streamGroup;
		this.streamName = streamName;
		this.jsonReplacer = jsonReplacer;

		bufferedLogs.set(`${streamGroup}{::}${streamName}`, []);

		// createLogStream(streamGroup, streamName);
	}

	private log(level: "debug" | "info" | "warn" | "error", data: LogMessage) {
		const obj = Object.assign({
			level: level.toUpperCase(),
			data,
		});

		const result = addLog(this.streamGroup, this.streamName, {
			timestamp: Date.now(),
			message: JSON.stringify(obj, this.jsonReplacer),
		});
		// TODO: log error to loggers errors
		if (result instanceof Error) {
			console.error(result);
		}
	}

	//

	debug(obj: LogMessage) {
		this.log("debug", obj);
	}

	info(obj: LogMessage) {
		this.log("info", obj);
	}

	warn(obj: LogMessage) {
		this.log("warn", obj);
	}

	error(obj: LogMessage) {
		this.log("error", obj);
	}
}
