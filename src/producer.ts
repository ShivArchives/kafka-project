import { kafka } from "./client";
import * as readline from "readline";

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function init() {
  const producer = kafka.producer();

  console.log("Producer connecting...");
  await producer.connect();
  console.log("Producer connected");

  rl.setPrompt("> ");
  rl.prompt();

  rl.on("line", async (line: string) => {
    const [name, location] = line.split(" ");

    await producer.send({
      topic: "rider-updates",
      messages: [
        {
          partition: location.toLowerCase() === "north" ? 0 : 1,
          key: "location-updates",
          value: JSON.stringify({ name, location }),
        },
      ],
    });
  }).on("close", async () => {
    console.log("Closing producer...");
    await producer.disconnect();
  });
}

init().catch(console.error);
