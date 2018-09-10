"use strict";

const argv = require("yargs")
  .command("replicate", "continuously sync a redis instance to another", {
    source: {
      alias: "s",
      demandOption: true,
      describe: "Copy data from this Redis instance"
    },
    destination: {
      alias: "d",
      demandOption: true,
      describe: "Send data to this Redis instance"
    }
  })
  .demandCommand(1, "You must specify a command")
  .help()
  .argv;
const redis = require("redis");
const util = require("util");

const commandsToRun = [
  "del",
  "expire",
  "hmset",
  "hset"
];

[commandsToRun].forEach((fnName) => {
  let rcp = redis.RedisClient.prototype;
  rcp[fnName + "Async"] = util.promisify(rcp[fnName]);
});

const main = async () => {
  const redis_options = { detect_buffers: true };
  let source_client = redis.createClient(argv.source, redis_options);
  let target_client = redis.createClient(argv.destination, redis_options);

  source_client.on("monitor", async (time, args) => {
    let command = args[0];
    let commandArgs = args.slice(1);
    
    if (commandsToRun.includes(command)) {
      console.log(time, args);
      let result = await target_client[command + "Async"](commandArgs);
      console.log(result)
    }
  });

  source_client.monitor();
};

main()





