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

export interface Log extends InputLogEvent {
	timestamp: number;
	message: string;
}

// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
// https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
const FIXED_EVENT_PREFIX = 26 /* byte */;
const MAX_EVENT_SIZE = 2 ** 18 - FIXED_EVENT_PREFIX /* byte */; // 256 KB - 26 bytes.

const MAX_BUFFER_LENGTH = 10_000;
const MAX_BUFFER_SIZE = 2 ** 20 /* byte */; // 1 MB

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
	interval: 1500,
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
		for (const logs of bufferedLogs.values()) {
			if (logs.length) {
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

	if (logs.length + 1 >= MAX_BUFFER_LENGTH) {
		return true;
	}

	const size = logs.reduce(
		(accumulator, log) => accumulator + Buffer.byteLength(log.message, "utf-8"),
		0,
	);

	const messageSize = Buffer.byteLength(newLog.message, "utf-8");

	return size + messageSize >= MAX_BUFFER_SIZE;
}

function logEventExceedsSize(log: Log): boolean {
	return Buffer.byteLength(log.message, "utf-8") >= MAX_EVENT_SIZE;
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

async function addErrorLog(
	groupName: string,
	streamName: string,
	errorLog: { message: string; error: string },
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
const createLogGroup = throttle(async (logGroupName: string): Promise<void> => {
	if (!client) {
		throw new Error("CloudWatchLogs `client` is not initialized.");
	}

	try {
		await client.send(new CreateLogGroupCommand({ logGroupName }));
	} catch (error) {
		if (isResourceAlreadyExistsException(error)) {
			return;
		}
		throw new Error("Create Log Group Failed", { cause: error });
	}
});

/**
 * @throws {Error} if CloudWatchLogs client is not initialized.
 */
const createLogStream = throttle(
	async (logGroupName: string, logStreamName: string): Promise<void> => {
		if (!client) {
			throw new Error("CloudWatchLogs `client` is not initialized.");
		}

		try {
			await createLogGroup(logGroupName);

			await client.send(
				new CreateLogStreamCommand({
					logGroupName,
					logStreamName,
				}),
			);

			const logGroup = logGroups.get(logGroupName);
			if (!logGroup) {
				logGroups.set(logGroupName, [logStreamName]);
			} else {
				logGroup.push(logStreamName);
			}
		} catch (error) {
			if (isResourceAlreadyExistsException(error)) {
				return;
			}

			throw new Error("Create Log Stream Failed", { cause: error });
		}
	},
);

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
		if (!logGroups.get(logGroupName)?.includes(logStreamName)) {
			await createLogStream(logGroupName, logStreamName);
		}

		await client.send(
			new PutLogEventsCommand({
				logEvents,
				logGroupName,
				logStreamName,
			}),
		);
	} catch (error) {
		throw new Error("Put Log Events Failed", { cause: error });
	}
});

/**
 * @throws {Error} if CloudWatchLogs throttle is not initialized.
 */
// ensure that logs have been _copied_ and cleared syncnorously
export function flush(): Promise<void> {
	const entries = getOrderedLogs();

	return _flush(entries);
}

async function _flush(
	entries: ReturnType<typeof getOrderedLogs>,
): Promise<void> {
	try {
		for (const [groupName, streamName, logs] of entries) {
			await putEventLogs(groupName, streamName, logs);
		}
	} catch (error) {
		console.error(error);
		await addErrorLog("cloudwatch_logger", "errors", {
			message: "flushing error",
			error: String(error),
		});
	}
}

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
				.slice(0, MAX_EVENT_SIZE)
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

		createLogStream(streamGroup, streamName);
	}

	private log(level: "debug" | "info" | "warn" | "error", data: LogMessage) {
		const obj = Object.assign({
			level: level.toUpperCase(),
			data,
		});

		void addLog(this.streamGroup, this.streamName, {
			timestamp: Date.now(),
			message: JSON.stringify(obj, this.jsonReplacer),
		});
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
