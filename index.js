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

const log = (msg) => {
  console.log(`${(new Date()).toISOString()} - ${msg}`)
}

const main = async () => {
  const redis_options = { detect_buffers: true };
  let source_client = redis.createClient(argv.source, redis_options);
  let destination_client = redis.createClient(argv.destination, redis_options);

  source_client.on("monitor", async (time, args) => {
    let command = args[0];
    let commandArgs = args.slice(1);

    if (commandsToRun.includes(command)) {
      log(`monitor recieved - ${args}`);
      let result = await destination_client[command + "Async"](commandArgs);
      log(`monitor applied  - ${result}`);
    } else {
      log(`monitor ignored  - ${args}`);
    }
  });

  source_client.monitor();

  let iterator = 0;

  do {
    log(`scan start - iterator ${iterator}`);
    let scan_result = await source_client.scanAsync(iterator);
    iterator = scan_result[0];
    log(`scan keys  - ${scan_result[1]}`);

    for (let key of scan_result[1]) {
      let multi_result = await source_client.multi().dump(new Buffer(key)).pttl(key).execAsync();
      let object = multi_result[0];
      let expiry = multi_result[1] >= 0 ? multi_result[1] : 0;
      log(`fetched - key ${key}`);

      let restore_result = await destination_client.restoreAsync(key, expiry, object, 'REPLACE');
      log(`restore complete - ${restore_result.toString()}`);
    }
  } while (iterator != 0)

  log(`initial sync complete, Ctrl-C to stop monitor sync`);
};

main()





