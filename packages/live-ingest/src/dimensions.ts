import type { Pool } from "pg";
import { lookupEmojiMetadata } from "./emojiMetadata.js";

export class DimensionCache {
  private languageByCode = new Map<string, number>();
  private clientByIdentifier = new Map<string, number>();
  private emojiByGlyph = new Map<string, number>();

  constructor(private readonly pool: Pool) {}

  async hydrate() {
    await Promise.all([
      this.loadLanguageCache(),
      this.loadClientCache(),
      this.loadEmojiCache(),
    ]);
  }

  private async loadLanguageCache() {
    const { rows } = await this.pool.query<{ lang_id: number; bcp47: string }>(
      "SELECT lang_id, bcp47 FROM dim_language",
    );
    for (const row of rows) {
      this.languageByCode.set(row.bcp47, row.lang_id);
    }
  }

  private async loadClientCache() {
    const { rows } = await this.pool.query<{
      client_id: number;
      identifier: string;
    }>("SELECT client_id, identifier FROM dim_client");
    for (const row of rows) {
      this.clientByIdentifier.set(row.identifier, row.client_id);
    }
  }

  private async loadEmojiCache() {
    const { rows } = await this.pool.query<{ emoji_id: number; glyph: string }>(
      "SELECT emoji_id, glyph FROM dim_emoji",
    );
    for (const row of rows) {
      this.emojiByGlyph.set(row.glyph, row.emoji_id);
    }
  }

  async getLanguageId(code: string): Promise<number> {
    const normalized = code.toLowerCase();
    const cached = this.languageByCode.get(normalized);
    if (cached) return cached;
    const { rows } = await this.pool.query<{ lang_id: number }>(
      `INSERT INTO dim_language (bcp47, display_name) VALUES ($1, $2)
	ON CONFLICT (bcp47) DO UPDATE SET display_name = EXCLUDED.display_name RETURNING lang_id`,
      [normalized, normalized],
    );
    const langId = rows[0].lang_id;
    this.languageByCode.set(normalized, langId);
    return langId;
  }

  async getClientId(identifier: string | null): Promise<number | null> {
    if (!identifier) return null;
    const cached = this.clientByIdentifier.get(identifier);
    if (cached) return cached;
    const { rows } = await this.pool.query<{ client_id: number }>(
      `INSERT INTO dim_client (identifier, display_name) VALUES ($1, $2)
	ON CONFLICT (identifier) DO UPDATE SET display_name = EXCLUDED.display_name RETURNING client_id`,
      [identifier, identifier],
    );
    const clientId = rows[0].client_id;
    this.clientByIdentifier.set(identifier, clientId);
    return clientId;
  }

  async getEmojiId(glyph: string): Promise<number> {
    const cached = this.emojiByGlyph.get(glyph);
    if (cached) return cached;
    const metadata = lookupEmojiMetadata(glyph);
    const { rows } = await this.pool.query<{ emoji_id: number }>(
      `INSERT INTO dim_emoji (glyph, group_name, shortcodes)
	VALUES ($1, $2, $3)
	ON CONFLICT (glyph) DO UPDATE SET group_name = EXCLUDED.group_name, shortcodes = EXCLUDED.shortcodes
	RETURNING emoji_id`,
      [glyph, metadata.groupName, metadata.shortcodes],
    );
    const emojiId = rows[0].emoji_id;
    this.emojiByGlyph.set(glyph, emojiId);
    return emojiId;
  }
}
