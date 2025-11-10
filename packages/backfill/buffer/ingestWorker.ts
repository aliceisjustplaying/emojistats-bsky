import { type Event, IndexerWorker, jsonToLex } from "@futur/bsky-indexer";

class IngestWorker extends IndexerWorker {
  // @ts-expect-error — should make IndexerWorker generic so we can pass in string instead of Uint8Array
  override process = async ({ line }: { line: string }) => {
    try {
      const event = jsonToLex(JSON.parse(line)) as Event;
      if (!event) return { success: true };
      const { success, cursor, error } = await this.tryIndexEvent(event);
      if (success) {
        return { success, cursor };
      } else {
        return { success, error };
      }
    } catch (err) {
      return { success: false, error: err };
    }
  };
}

export default new IngestWorker();
