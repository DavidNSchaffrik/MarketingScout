import sys, os, asyncio
mode = sys.argv[1]
worker_id = sys.argv[2]
disc_count = sys.argv[3] if len(sys.argv) > 3 else "1"

os.environ["CRAWLER_MODE"] = mode
os.environ["WORKER_ID"] = worker_id
os.environ["DISCOVERY_COUNT"] = disc_count

from bhw_prototype import *

if mode == "discovery":
    asyncio.run(run_forum_discovery(worker_id=int(worker_id)))
else:
    asyncio.run(run_thread_worker(worker_id=int(worker_id)))