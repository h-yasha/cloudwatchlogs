import { setTimeout } from "node:timers/promises";
// import { setup, addLog, flush } from "@purpleduck/cloudwatch-logs";
import { Logger, setup } from "../index.ts";
import { nanoid } from "nanoid";
import cryptoRandomString from "crypto-random-string";

console.log("Hello via Bun!");

setup({
	flushInterval: 1000,
	region: "me-south-1",
});

const logGroup = "logger-tests";

let i = 0;

const routes = ["/", "/wallet", "/v2/wallet/top-up", "/v4"];

function getLogger(max = 2049) {
	i++;

	if (i === max) {
		i = 0;
	}

	return new Logger(
		logGroup,
		`${routes[i % 3]}/${cryptoRandomString({ length: 30 })}`,
	);
}

let j = 0;

const logs = [];

while (j < 10_000) {
	const v = {
		message: crypto.randomUUID(),
		value: nanoid(2048),
	};
	const logger = getLogger();
	logs.push([logger.streamName, v]);
	logger.info(v);
	// j % 200 === 0 && (await setTimeout(1));
	j++;
}

Bun.write("test.json", JSON.stringify(logs, null, 2));
