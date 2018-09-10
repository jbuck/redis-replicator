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

["dump", "restore", "scan"].forEach((fnName) => {
  let rcp = redis.RedisClient.prototype;
  rcp[fnName + "Async"] = util.promisify(rcp[fnName]);
});
["exec"].forEach((fnName) => {
  let rmp = redis.Multi.prototype;
  rmp[fnName + "Async"] = util.promisify(rmp[fnName]);
});

const allowedCommands = [
  "APPEND", // key value
  "BITFIELD", // key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL]
  "", //
]

const main = async () => {
  const redis_options = { detect_buffers: true };
  let source_client = redis.createClient(argv.source, redis_options);
  let target_client = redis.createClient(argv.destination, redis_options);

  let iterator = 0;

  do {
    let scan_result = await source_client.scanAsync(iterator);
    iterator = scan_result[0];
    console.log(scan_result)

    for (let key of scan_result[1]) {
      let multi_result = await source_client.multi().dump(new Buffer(key)).pttl(key).execAsync();
      let object = multi_result[0];

      let expiry = multi_result[1] >= 0 ? multi_result[1] : 0;
      console.log(multi_result)

      let restore_result = await target_client.restoreAsync(key, expiry, object, 'REPLACE');
      console.log(restore_result)
    }
  } while (iterator != 0)

  source_client.quit()
  target_client.quit()
};

main()





