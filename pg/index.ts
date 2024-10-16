import { setTimeout } from "node:timers/promises";
// import { setup, addLog, flush } from "@purpleduck/cloudwatch-logs";
import { Logger, setup } from "../index.ts";

console.log("Hello via Bun!");

setup({
	flushInterval: 1000,
	region: "me-south-1",
});

const logGroup = "logger-tests";

const logger1 = new Logger(logGroup, "stream1");
const logger2 = new Logger(logGroup, "stream2");
const logger3 = new Logger(logGroup, "stream3");
const logger4 = new Logger(logGroup, "stream4");
const logger5 = new Logger(logGroup, "stream5");

const loggers = [logger1, logger2, logger3, logger4, logger5];

let i = 0;
function getLogger(max = 5) {
	i++;
	if (i === max) {
		i = 0;
	}

	// biome-ignore lint/style/noNonNullAssertion: <explanation>
	return loggers[i]!;
}

let j = 0;

while (j < 50) {
	getLogger(3).info({ message: j });
	j++;
}

await setTimeout(3000);

while (j < 50) {
	getLogger(3).info({ message: j });
	j++;
}

while (j < 500) {
	getLogger().info({ message: j });
	await setTimeout(250);
	j++;
}
