// import { setup, addLog, flush } from "@purpleduck/cloudwatch-logs";
import { setup, addLog, flush } from "../index.ts";

console.log("Hello via Bun!");

setup({ flushInterval: 1000, region: "me-south-1" });

let s = "";
while (Buffer.byteLength(s) < 2 ** 20) {
	s += Math.random().toString();
	s += Math.random().toString();
	s += Math.random().toString();
	s += Math.random().toString();
	s += Math.random().toString();
	s += Math.random().toString();
	s += Math.random().toString();
}

console.log(addLog("test", "test", { timestamp: Date.now(), message: "test" }));

await flush();

console.log(addLog("test", "test", { timestamp: Date.now(), message: "test" }));
console.log(addLog("test", "test", { timestamp: Date.now(), message: "test" }));
console.log(addLog("test", "test", { timestamp: Date.now(), message: "test" }));

console.log(addLog("test", "test", { timestamp: Date.now(), message: s }));

//
// await flush();
//
// // process.on("beforeExit", async () => {
// // 	flush
// //
// // })
// //
