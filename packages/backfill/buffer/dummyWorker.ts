import { ThreadWorker } from "@futur/bsky-indexer";

class DummyWorker extends ThreadWorker {
  constructor() {
    super(() => {});
  }
}

export default new DummyWorker();
