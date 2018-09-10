"use strict";

// Setup CLI argument parsing
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

// These are redis commands that send uses for inserting/updating/deleting/expiring data
const commandsToRun = [
  "del",
  "expire",
  "hmset",
  "hset"
];

// This adds Promise-returning equivalent commands to the redis protoype for use with async/await
commandsToRun.forEach((fnName) => {
  let rcp = redis.RedisClient.prototype;
  rcp[fnName + "Async"] = util.promisify(rcp[fnName]);
});
["dump", "restore", "scan"].forEach((fnName) => {
  let rcp = redis.RedisClient.prototype;
  rcp[fnName + "Async"] = util.promisify(rcp[fnName]);
});
["exec"].forEach((fnName) => {
  let rmp = redis.Multi.prototype;
  rmp[fnName + "Async"] = util.promisify(rmp[fnName]);
});

// Just a simple logger with timestamp
const log = (msg) => {
  console.log(`${(new Date()).toISOString()} - ${msg}`)
}

const main = async () => {
  // detect_buffers uses Buffers to transfer data with dump/restore correctly
  const redis_options = { detect_buffers: true };
  let source_client = redis.createClient(argv.source, redis_options);
  let destination_client = redis.createClient(argv.destination, redis_options);

  // Listen for changes to the source redis via monitor command
  source_client.on("monitor", async (time, args) => {
    let command = args[0];
    let commandArgs = args.slice(1);

    // We only need to apply data modification commands to the destination redis
    if (commandsToRun.includes(command)) {
      log(`monitor recieved - ${args}`);
      let result = await destination_client[command + "Async"](commandArgs);
      log(`monitor applied  - ${result}`);
    }
  });

  source_client.monitor();

  // Redis scan iterators start at 0, and end when it returns 0
  let iterator = 0;

  do {
    log(`scan start - iterator ${iterator}`);
    let scan_result = await source_client.scanAsync(iterator);
    iterator = scan_result[0];
    log(`scan keys  - ${scan_result[1]}`);

    for (let key of scan_result[1]) {
      // This returns the data structure of any key as binary data & the associated TTL in the same command
      let multi_result = await source_client.multi().dump(new Buffer(key)).pttl(key).execAsync();
      let object = multi_result[0];
      let expiry = multi_result[1] >= 0 ? multi_result[1] : 0;
      log(`fetched - key ${key}`);

      // And this restores the data to the destination redis
      let restore_result = await destination_client.restoreAsync(key, expiry, object, 'REPLACE');
      log(`restore complete - ${restore_result.toString()}`);
    }
  } while (iterator != 0)

  log(`initial sync complete, Ctrl-C to stop monitor sync`);
};

main()





