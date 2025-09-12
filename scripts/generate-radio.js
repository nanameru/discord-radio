#!/usr/bin/env node
import fs from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { JSDOM } from 'jsdom';
import { Readability } from '@mozilla/readability';
import pLimit from 'p-limit';
import OpenAI from 'openai';

// Configuration via environment variables
const discordBotToken = process.env.DISCORD_BOT_TOKEN;
const discordChannelIdsEnv = process.env.DISCORD_CHANNEL_IDS || '';
const aggregationMode = process.env.AGGREGATION_MODE || 'single'; // 'single' | 'per_channel' (single default)
const maxArticleCount = parseInt(process.env.MAX_URLS || '20', 10);
const maxArticleChars = parseInt(process.env.MAX_TEXT_CHARS || '2000', 10);
const maxConcurrency = parseInt(process.env.MAX_CONCURRENCY || '4', 10);
const logLevel = process.env.LOG_LEVEL || 'info';
const logDiscordDump = String(process.env.LOG_DISCORD_DUMP || '').toLowerCase() === 'true' || process.env.LOG_DISCORD_DUMP === '1';
// Optional forum tags (comma separated Snowflakes) applied when posting to Forum
const discordForumTagIdsEnv = process.env.DISCORD_FORUM_TAG_IDS || '';

// MiniMax T2A configuration
const minimaxApiKey = process.env.MINIMAX_API_KEY;
const minimaxGroupId = process.env.MINIMAX_GROUP_ID; // often required by MiniMax as X-Group-ID
const minimaxModel = process.env.MINIMAX_T2A_MODEL || 'speech-2.5-hd-preview';
const minimaxVoiceSingle = process.env.MINIMAX_VOICE_ID || process.env.MINIMAX_VOICE_ID_A || process.env.MINIMAX_VOICE_ID_B || '';
const minimaxAudioFormat = process.env.MINIMAX_AUDIO_FORMAT || 'mp3';
const minimaxSpeed = Number(process.env.MINIMAX_SPEED || '1.0');
const minimaxEndpoint = process.env.MINIMAX_T2A_URL || 'https://api.minimax.io/v1/t2a_v2';

// OpenAI config
const openaiApiKey = process.env.OPENAI_API_KEY || '';
const openaiModel = process.env.OPENAI_MODEL || 'gpt-4o-mini';
const openai = openaiApiKey ? new OpenAI({ apiKey: openaiApiKey }) : null;

function logInfo(message, ...rest) {
  if (['info', 'debug'].includes(logLevel)) {
    console.log(`[INFO] ${message}`, ...rest);
  }
}

function logDebug(message, ...rest) {
  if (logLevel === 'debug') {
    console.log(`[DEBUG] ${message}`, ...rest);
  }
}

function logWarn(message, ...rest) {
  console.warn(`[WARN] ${message}`, ...rest);
}

function logError(message, ...rest) {
  console.error(`[ERROR] ${message}`, ...rest);
}

function assertRequiredEnv() {
  const missing = [];
  if (!discordBotToken) missing.push('DISCORD_BOT_TOKEN');
  if (!discordChannelIdsEnv) missing.push('DISCORD_CHANNEL_IDS');
  if (!minimaxApiKey) missing.push('MINIMAX_API_KEY');
  if (!minimaxGroupId) missing.push('MINIMAX_GROUP_ID');
  if (!minimaxVoiceSingle) missing.push('MINIMAX_VOICE_ID');
  if (!openaiApiKey) missing.push('OPENAI_API_KEY');
  if (missing.length) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

function parseCliArgs() {
  // Supports: --date YYYY-MM-DD (JST date for end boundary 04:00 JST)
  const args = process.argv.slice(2);
  const argMap = new Map();
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg.startsWith('--')) {
      const key = arg.slice(2);
      const value = args[i + 1] && !args[i + 1].startsWith('--') ? args[++i] : 'true';
      argMap.set(key, value);
    }
  }
  return {
    date: argMap.get('date') || null,
    noPost: argMap.has('no-post'),
    postOnly: argMap.has('post-only'),
  };
}

// Time helpers (JST = UTC+9, no DST)
function buildJstEndUtcFromDateString(dateStr /* YYYY-MM-DD */) {
  const [yearStr, monthStr, dayStr] = dateStr.split('-');
  const year = parseInt(yearStr, 10);
  const month = parseInt(monthStr, 10);
  const day = parseInt(dayStr, 10);
  // 04:00 JST => 19:00 UTC (previous day)
  const endUtc = new Date(Date.UTC(year, month - 1, day, 4 - 9, 0, 0, 0));
  return endUtc;
}

function getDefaultJstEndUtcNow() {
  const nowUtc = new Date();
  const nowJstMs = nowUtc.getTime() + 9 * 60 * 60 * 1000;
  const nowJst = new Date(nowJstMs);
  const year = nowJst.getUTCFullYear();
  const month = nowJst.getUTCMonth();
  const day = nowJst.getUTCDate();
  const hours = nowJst.getUTCHours();
  // Most recent 04:00 JST boundary
  let endJstBase = new Date(Date.UTC(year, month, day, 4, 0, 0, 0));
  if (hours < 4) {
    // Use yesterday's 04:00 JST
    endJstBase = new Date(endJstBase.getTime() - 24 * 60 * 60 * 1000);
  }
  // Convert JST to UTC by subtracting 9h
  const endUtc = new Date(endJstBase.getTime() - 9 * 60 * 60 * 1000);
  return endUtc;
}

function toIsoNoMs(date) {
  return new Date(date.getTime() - date.getMilliseconds()).toISOString();
}

function formatJst(dateUtc) {
  const jstMs = dateUtc.getTime() + 9 * 60 * 60 * 1000;
  const jst = new Date(jstMs);
  const yyyy = jst.getUTCFullYear();
  const mm = String(jst.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(jst.getUTCDate()).padStart(2, '0');
  const HH = String(jst.getUTCHours()).padStart(2, '0');
  const MM = String(jst.getUTCMinutes()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd} ${HH}:${MM} JST`;
}

function extractUrlsFromString(text) {
  if (!text) return [];
  const urlRegex = /https?:\/\/[^\s<>()\[\]"']+/g;
  const matches = text.match(urlRegex);
  return matches ? matches : [];
}

function normalizeUrl(rawUrl) {
  try {
    const url = new URL(rawUrl);
    url.hash = '';
    // Remove known tracking params
    const paramsToRemove = [
      'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
      'gclid', 'fbclid'
    ];
    for (const key of paramsToRemove) {
      url.searchParams.delete(key);
    }
    // Remove trailing slash for root/paths
    let pathname = url.pathname;
    if (pathname.endsWith('/') && pathname.length > 1) {
      pathname = pathname.slice(0, -1);
    }
    url.pathname = pathname;
    url.hostname = url.hostname.toLowerCase();
    return url.toString();
  } catch {
    return rawUrl;
  }
}

function isXStatusUrl(rawUrl) {
  try {
    const u = new URL(rawUrl);
    const host = u.hostname.toLowerCase();
    return (host.endsWith('x.com') || host.endsWith('twitter.com')) && /\/status\//.test(u.pathname);
  } catch {
    return false;
  }
}

async function fetchXoEmbed(rawUrl) {
  const api = `https://publish.twitter.com/oembed?omit_script=1&hide_thread=1&hide_media=0&dnt=1&url=${encodeURIComponent(rawUrl)}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12000);
  try {
    const res = await fetch(api, { signal: controller.signal });
    if (!res.ok) throw new Error(`oEmbed ${res.status}`);
    const data = await res.json();
    const html = data?.html || '';
    // Convert embed HTML to text
    const dom = new JSDOM(`<div>${html}</div>`);
    const textContent = dom.window.document.body.textContent || '';
    const clean = textContent.replace(/\s+/g, ' ').trim();
    const author = data?.author_name ? `（by ${data.author_name}）` : '';
    const title = clean.slice(0, 40) + author;
    return { title, text: clean };
  } catch (e) {
    logDebug(`X oEmbed failed for ${rawUrl}: ${e?.message || e}`);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function discordApiRequest(pathname, queryParams = {}) {
  const baseUrl = 'https://discord.com/api/v10';
  const url = new URL(baseUrl + pathname);
  for (const [key, value] of Object.entries(queryParams)) {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  }
  while (true) {
    const res = await fetch(url, {
      headers: {
        'Authorization': `Bot ${discordBotToken}`,
        'Content-Type': 'application/json'
      }
    });
    if (res.status === 429) {
      const retryAfter = Number(res.headers.get('retry-after')) || 1;
      logWarn(`Discord rate limited. Retrying after ${retryAfter}s`);
      await delay(retryAfter * 1000);
      continue;
    }
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Discord API error ${res.status}: ${body}`);
    }
    return res.json();
  }
}

async function discordGetChannel(channelId) {
  const baseUrl = 'https://discord.com/api/v10';
  const url = new URL(baseUrl + `/channels/${channelId}`);
  while (true) {
    const res = await fetch(url, {
      headers: {
        'Authorization': `Bot ${discordBotToken}`,
        'Content-Type': 'application/json'
      }
    });
    if (res.status === 429) {
      const retryAfter = Number(res.headers.get('retry-after')) || 1;
      logWarn(`Discord rate limited. Retrying after ${retryAfter}s`);
      await delay(retryAfter * 1000);
      continue;
    }
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Discord API error ${res.status}: ${body}`);
    }
    return res.json();
  }
}

async function discordGetGuildActiveThreads(guildId) {
  // https://discord.com/developers/docs/resources/guild#list-active-threads
  return discordApiRequest(`/guilds/${guildId}/threads/active`);
}

async function discordGetForumArchivedPublicThreads(channelId, params = {}) {
  // https://discord.com/developers/docs/resources/channel#list-public-archived-threads
  return discordApiRequest(`/channels/${channelId}/threads/archived/public`, params);
}

async function fetchChannelMessagesInRange(channelId, startUtc, endUtc) {
  const collected = [];
  let before = undefined; // message id to paginate backwards
  while (true) {
    const params = { limit: 100 };
    if (before) params.before = before;
    const page = await discordApiRequest(`/channels/${channelId}/messages`, params);
    if (!Array.isArray(page) || page.length === 0) break;

    // Sort oldest->newest to make range filtering predictable when paginating
    page.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    let reachedOlderThanStart = false;
    for (const msg of page) {
      const ts = new Date(msg.timestamp);
      if (ts >= startUtc && ts < endUtc) {
        collected.push(msg);
      }
      if (ts < startUtc) {
        reachedOlderThanStart = true;
      }
    }

    // Prepare next pagination cursor (before = oldest message id in this page)
    before = page[0].id;

    if (reachedOlderThanStart) break;
    if (page.length < 100) break; // No more pages
  }
  return collected;
}

async function fetchForumChannelMessagesInRange(forumChannelId, startUtc, endUtc) {
  // Collect thread IDs under the forum channel within/around the window, then fetch their messages
  const threads = new Map();
  try {
    const ch = await discordGetChannel(forumChannelId).catch(() => null);
    const guildId = ch && ch.guild_id ? ch.guild_id : null;
    // 1) Active threads at guild level, filter by parent
    if (guildId) {
      try {
        const active = await discordGetGuildActiveThreads(guildId);
        const actThreads = Array.isArray(active?.threads) ? active.threads : [];
        for (const t of actThreads) {
          if (t?.parent_id === forumChannelId) {
            threads.set(t.id, t);
          }
        }
      } catch (e) {
        logDebug(`Failed to list active threads for guild ${guildId}: ${e?.message || e}`);
      }
    }
    // 2) Archived public threads for the forum channel, paginate by 'before' timestamp
    let before = undefined; // ISO8601 timestamp string
    for (let i = 0; i < 10; i++) { // hard stop to avoid infinite loops
      const params = { limit: 100 };
      if (before) params.before = before;
      let resp = null;
      try {
        resp = await discordGetForumArchivedPublicThreads(forumChannelId, params);
      } catch (e) {
        logDebug(`Failed to list archived threads for forum ${forumChannelId}: ${e?.message || e}`);
        break;
      }
      const arr = Array.isArray(resp?.threads) ? resp.threads : [];
      if (arr.length === 0) break;
      for (const t of arr) {
        // Only keep threads whose archive_timestamp or last_message_id time intersects the window roughly
        // Use 'archive_timestamp' as a proxy for recency
        const archiveTs = t?.thread_metadata?.archive_timestamp || t?.archive_timestamp;
        if (archiveTs) {
          const at = new Date(archiveTs);
          if (at >= startUtc || at >= new Date(startUtc.getTime() - 7 * 24 * 60 * 60 * 1000)) { // include near range
            threads.set(t.id, t);
          }
        } else {
          threads.set(t.id, t);
        }
      }
      const last = arr[arr.length - 1];
      const lastTs = last?.thread_metadata?.archive_timestamp || last?.archive_timestamp;
      if (lastTs) {
        before = lastTs;
        if (new Date(lastTs) < startUtc) break; // older than start
      } else {
        break;
      }
      if (!resp?.has_more) break;
    }
  } catch (e) {
    logDebug(`Forum thread discovery failed: ${e?.message || e}`);
  }

  const threadIds = Array.from(threads.keys());
  logInfo(`Forum ${forumChannelId}: discovered ${threadIds.length} threads (active/archived)`);
  const allMessages = [];
  for (const tid of threadIds) {
    try {
      const msgs = await fetchChannelMessagesInRange(tid, startUtc, endUtc);
      if (msgs?.length) allMessages.push(...msgs);
    } catch (e) {
      logDebug(`Failed to fetch messages for thread ${tid}: ${e?.message || e}`);
    }
  }
  return allMessages;
}

function filterAndExtractFromMessages(messages) {
  const results = [];
  for (const msg of messages) {
    // Ignore system messages and bots if possible
    if (msg.type && msg.type !== 0) continue; // 0: default message
    if (msg.author && msg.author.bot) continue;

    const textParts = [];
    if (msg.content) {
      textParts.push(msg.content);
    }
    // Attachments
    if (Array.isArray(msg.attachments)) {
      for (const att of msg.attachments) {
        if (att.url) textParts.push(att.url);
      }
    } else if (msg.attachments && typeof msg.attachments === 'object') {
      // Discord API returns attachments as array; just in case
      for (const att of Object.values(msg.attachments)) {
        if (att && att.url) textParts.push(att.url);
      }
    }
    // Embeds
    if (Array.isArray(msg.embeds)) {
      for (const emb of msg.embeds) {
        if (emb.url) textParts.push(emb.url);
        if (emb.title) textParts.push(emb.title);
        if (emb.description) textParts.push(emb.description);
      }
    }

    const combined = textParts.join('\n');
    const urls = extractUrlsFromString(combined).map(normalizeUrl);
    results.push({
      id: msg.id,
      author: msg.author ? { id: msg.author.id, username: msg.author.username } : null,
      timestamp: msg.timestamp,
      content: msg.content || '',
      urls: Array.from(new Set(urls)),
    });
  }
  return results;
}

async function fetchArticleContent(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    // X/Twitter: try oEmbed without fetching the page
    if (isXStatusUrl(url)) {
      const xo = await fetchXoEmbed(url);
      if (xo) {
        return { url, title: xo.title || 'Xの投稿', text: xo.text || null };
      }
    }

    const res = await fetch(url, { signal: controller.signal, redirect: 'follow' });
    const contentType = res.headers.get('content-type') || '';
    if (!contentType.includes('text/html')) {
      return { url, title: null, text: null };
    }
    const html = await res.text();
    const dom = new JSDOM(html, { url });
    const reader = new Readability(dom.window.document);
    const article = reader.parse();
    if (!article) {
      // Fallback: basic text content
      const fallbackText = dom.window.document.body && dom.window.document.body.textContent
        ? dom.window.document.body.textContent
        : '';
      return {
        url,
        title: dom.window.document.title || null,
        text: fallbackText ? fallbackText.trim().slice(0, maxArticleChars) : null,
      };
    }
    const cleanText = article.textContent ? article.textContent.replace(/\s+/g, ' ').trim() : null;
    return {
      url,
      title: article.title || dom.window.document.title || null,
      text: cleanText ? cleanText.slice(0, maxArticleChars) : null,
    };
  } catch (err) {
    logWarn(`Failed to fetch article: ${url} (${err?.message || err})`);
    return { url, title: null, text: null };
  } finally {
    clearTimeout(timeout);
  }
}

function buildMonologueScriptHeuristic(perChannelMaterials, { startUtc, endUtc }) {
  const startJstLabel = formatJst(startUtc);
  const endJstLabel = formatJst(endUtc);
  const bullets = [];
  const textSnippets = [];
  for (const [, material] of perChannelMaterials) {
    // 記事（URL付き）
    for (const art of material.articles) {
      const title = art.title || '無題の記事';
      const summary = art.text ? art.text.slice(0, 240).replace(/\s+/g, ' ') : '';
      bullets.push({ title, url: art.url, summary });
    }
    // Discordのテキストのみの会話
    if (Array.isArray(material.texts)) {
      for (const msg of material.texts) {
        const snippet = (msg || '').replace(/\s+/g, ' ').trim();
        if (snippet) textSnippets.push(snippet.slice(0, 200));
      }
    }
  }
  // テキスト抜粋は多すぎないよう上限
  const limitedTextSnippets = textSnippets.slice(0, 20);

  const lines = [];
  lines.push(`こんにちは。本放送では、${startJstLabel} から ${endJstLabel} の間に共有された話題をまとめてご紹介します。`);
  
  if (bullets.length === 0 && limitedTextSnippets.length === 0) {
    // 充実したフォールバック台本
    lines.push('本日は特筆すべき新着トピックはありませんでした。しかし、この機会に最近の技術動向や業界の話題を振り返ってみたいと思います。');
    lines.push('');
    lines.push('まず、AI技術の進歩についてです。生成AIの分野では、大規模言語モデルの性能向上が続いており、より自然で実用的な対話が可能になってきています。');
    lines.push('また、マルチモーダルAIの発展により、テキスト、画像、音声を統合した処理が一般的になりつつあります。');
    lines.push('');
    lines.push('開発分野では、クラウドネイティブ技術の採用が加速しています。コンテナ化、マイクロサービス、サーバーレス等の技術により、');
    lines.push('スケーラブルで保守性の高いシステム構築が主流となっています。');
    lines.push('');
    lines.push('セキュリティ面では、ゼロトラスト・アーキテクチャの導入が進んでおり、従来の境界防御から脱却した新しいセキュリティモデルが注目されています。');
    lines.push('');
    lines.push('Web開発においては、フロントエンド・フレームワークの進化が続いており、パフォーマンスとDXの両立を図る技術が数多く登場しています。');
    lines.push('');
    lines.push('最後に、持続可能な開発への関心も高まっており、グリーンソフトウェア開発やカーボンニュートラルなインフラ運用が重要なテーマとなっています。');
    lines.push('');
    lines.push('これらの動向は、今後の技術選択や学習計画を考える上で重要な指針となるでしょう。');
  } else {
    lines.push('まずはDiscordでの会話から、要点を拾っていきます。');
    // 先に会話の抜粋
    if (limitedTextSnippets.length > 0) {
      for (const s of limitedTextSnippets) {
        lines.push(`・${s}`);
      }
    }
    // 続いて参考トピック（記事要約）
    if (bullets.length > 0) {
      lines.push('参考トピックの要点も簡単に。');
      let idx = 1;
      for (const b of bullets) {
        lines.push(`${idx}. ${b.title}`);
        if (b.summary) lines.push(`概要: ${b.summary}`);
        idx += 1;
      }
    }
  }
  
  lines.push('以上、注目の話題をダイジェストでお届けしました。');
  lines.push('この放送が情報収集の助けになれば幸いです。それでは、良い一日をお過ごしください。');
  return lines.join('\n');
}

async function buildMonologueScriptAI(perChannelMaterials, { startUtc, endUtc }) {
  // Fallback to heuristic if OpenAI not configured
  if (!openai) return buildMonologueScriptHeuristic(perChannelMaterials, { startUtc, endUtc });

  const startJstLabel = formatJst(startUtc);
  const endJstLabel = formatJst(endUtc);
  const topics = [];
  const chatSnippets = [];
  for (const [, material] of perChannelMaterials) {
    // URL付きの記事
    for (const art of material.articles) {
      topics.push({ title: art.title || '無題の記事', url: art.url, summary: art.text ? art.text.slice(0, 800) : '' });
    }
    // Discordのテキスト（URLなし）
    if (Array.isArray(material.texts)) {
      for (const msg of material.texts) {
        const s = (msg || '').replace(/\s+/g, ' ').trim();
        if (s) chatSnippets.push(s.slice(0, 300));
      }
    }
  }
  const limitedChatSnippets = chatSnippets.slice(0, 30);

  const system = 'あなたは日本語のナレーターです。Discordの会話ログを中心に、ポッドキャスト用の落ち着いた独白台本を作成します。専門用語は平易に説明し、冗長さは避け、5〜9分程度の長さを目安にしてください。';
  const chatsBlock = limitedChatSnippets.length > 0
    ? `Discordの会話抜粋:\n${limitedChatSnippets.map(s => `- ${s}`).join('\n')}`
    : 'Discordの会話抜粋: なし';
  const topicsBlock = topics.length > 0
    ? `参考資料（記事の要旨）:\n${topics.map(t => `- ${t.title}\n  要旨: ${t.summary}`).join('\n')}`
    : '参考資料: なし';
  const user = `期間: ${startJstLabel} 〜 ${endJstLabel}\n\n${chatsBlock}\n\n${topicsBlock}\n\n要件:\n- 主素材はDiscordの会話ログ。記事要約は補強として活用\n- 導入→要点整理（会話の流れを重視）→締めの順に\n- URLや出典の読み上げは不要（言及しない）\n- 台本のみを出力（ヘッダ不要）`;

  try {
    const resp = await openai.chat.completions.create({
      model: openaiModel,
      messages: [
        { role: 'system', content: system },
        { role: 'user', content: user },
      ],
      temperature: 0.7,
      max_tokens: 1200,
    });
    const content = resp.choices?.[0]?.message?.content?.trim();
    if (content) return content;
  } catch (e) {
    logWarn(`OpenAI generation failed, using heuristic: ${e?.message || e}`);
  }
  return buildMonologueScriptHeuristic(perChannelMaterials, { startUtc, endUtc });
}

// (Castmake removed)

async function generatePostTitleAI(perChannelMaterials, { startUtc, endUtc }) {
  // Prefer OpenAI; fallback to heuristic
  const defaultTitle = `日次ダイジェスト ${formatJst(startUtc).slice(0, 10)}`;
  try {
    const topics = [];
    const chatPhrases = [];
    for (const [, material] of perChannelMaterials) {
      for (const art of material.articles) {
        topics.push(art.title || '無題');
      }
      if (Array.isArray(material.texts)) {
        for (const t of material.texts) {
          const s = (t || '').replace(/\s+/g, ' ').trim();
          if (s) chatPhrases.push(s.slice(0, 40));
        }
      }
    }
    if (!openai) return topics[0] || chatPhrases[0] || defaultTitle;
    const tBlock = topics.length ? `記事トピック: ${topics.slice(0, 6).join(' / ')}` : '';
    const cBlock = chatPhrases.length ? `会話キーワード: ${chatPhrases.slice(0, 10).join(' / ')}` : '';
    const prompt = `以下の情報から、日本語の短い番組タイトルを1つ作ってください。最大28文字。装飾や引用符は不要で、具体的で簡潔に。\n${tBlock}${tBlock && cBlock ? '\n' : ''}${cBlock}\n期間: ${formatJst(startUtc)} 〜 ${formatJst(endUtc)}`;
    const resp = await openai.chat.completions.create({
      model: openaiModel,
      messages: [
        { role: 'system', content: 'あなたは日本語の見出しコピーライターです。短く端的で、ニュース要約向けの自然なタイトルを返します。' },
        { role: 'user', content: prompt },
      ],
      temperature: 0.6,
      max_tokens: 64,
    });
    const title = resp.choices?.[0]?.message?.content?.trim();
    return title || topics[0] || chatPhrases[0] || defaultTitle;
  } catch {
    return defaultTitle;
  }
}

async function main() {
  assertRequiredEnv();
  const { date, noPost, postOnly } = parseCliArgs();
  const endUtc = date ? buildJstEndUtcFromDateString(date) : getDefaultJstEndUtcNow();
  const startUtc = new Date(endUtc.getTime() - 24 * 60 * 60 * 1000);

  logInfo(`Time window (JST): ${formatJst(startUtc)} -> ${formatJst(endUtc)}`);

  // Allow multiple delimiters: comma, semicolon, Japanese punctuation, whitespace
  const discordChannelIds = discordChannelIdsEnv
    .split(/[ ,;\uFF0C\uFF1B、\s]+/)
    .map(s => s.trim())
    .filter(Boolean);

  if (discordChannelIds.length === 0) {
    throw new Error('No DISCORD_CHANNEL_IDS provided');
  }

  // If post-only: skip fetching/generation, just try to post latest audio
  if (postOnly) {
    await postLatestAudioFromOut(startUtc, endUtc);
    return;
  }

  const postingChannelId = discordChannelIds[0];
  const collectionChannelIds = discordChannelIds.slice(1);
  if (collectionChannelIds.length === 0) {
    logWarn('Only one DISCORD_CHANNEL_IDS provided. Using the first as both posting and collection source.');
  }

  const perChannelMaterials = new Map();
  const channelsToFetch = collectionChannelIds.length > 0 ? collectionChannelIds : [postingChannelId];
  logInfo(`DEBUG: Time range UTC: ${startUtc.toISOString()} -> ${endUtc.toISOString()}`);
  logInfo(`DEBUG: Time range JST: ${formatJst(startUtc)} -> ${formatJst(endUtc)}`);
  logInfo(`DEBUG: Channels to fetch: ${channelsToFetch.join(', ')}`);
  
  for (const channelId of channelsToFetch) {
    logInfo(`Fetching Discord messages for channel ${channelId}...`);
    let messages = [];
    try {
      const ch = await discordGetChannel(channelId).catch(() => null);
      logInfo(`DEBUG: Channel ${channelId} info:`, ch ? `type=${ch.type}, name=${ch.name || 'N/A'}` : 'null');
      if (ch && ch.type === 15) {
        // Forum channel: aggregate messages from threads
        messages = await fetchForumChannelMessagesInRange(channelId, startUtc, endUtc);
      } else {
        messages = await fetchChannelMessagesInRange(channelId, startUtc, endUtc);
      }
    } catch (e) {
      logWarn(`Failed to fetch messages for channel ${channelId}: ${e?.message || e}`);
      messages = [];
    }
    logInfo(`Fetched ${messages.length} messages in range for channel ${channelId}`);

    const extracted = filterAndExtractFromMessages(messages);
    logInfo(`DEBUG: Channel ${channelId} - Raw messages: ${messages.length}, After filtering: ${extracted.length}`);
    
    const allUrls = Array.from(new Set(extracted.flatMap(m => m.urls)));
    const limitedUrls = allUrls.slice(0, maxArticleCount);
    // すべてのテキスト（URL有無問わず）
    const textMessages = extracted.map(m => m.content).filter(Boolean);
    // ログ用にURL無しのテキスト件数も併記
    const nonUrlMessages = extracted.filter(m => (!m.urls || m.urls.length === 0)).map(m => m.content).filter(Boolean);

    // Logging: URL/テキストの内訳を残す
    const urlMsgCount = extracted.filter(m => (m.urls && m.urls.length > 0)).length;
    const textOnlyCount = extracted.filter(m => (!m.urls || m.urls.length === 0) && m.content).length;
    logInfo(`Channel ${channelId}: ${urlMsgCount} messages contained URLs, ${textOnlyCount} text-only messages, ${textMessages.length} total text messages`);
    logInfo(`DEBUG: Channel ${channelId} - URLs found: ${allUrls.length}, Text messages: ${textMessages.length}`);
    if (logLevel === 'debug') {
      const showUrls = allUrls.slice(0, 10);
      if (showUrls.length) logDebug(`Channel ${channelId}: sample URLs ->`, showUrls);
      const textSnippets = textMessages
        .map(t => (t || '').replace(/\s+/g, ' ').trim().slice(0, 120))
        .filter(Boolean)
        .slice(0, 5);
      if (textSnippets.length) logDebug(`Channel ${channelId}: sample text-only snippets ->`, textSnippets);
    }

    // Detect likely missing Message Content Intent / permissions
    try {
      const hasAnyContent = extracted.some(m => (m.content && m.content.trim().length > 0));
      const hasAnyUrls = extracted.some(m => (m.urls && m.urls.length > 0));
      if (messages.length > 0 && !hasAnyContent && !hasAnyUrls) {
        logWarn(
          `Channel ${channelId}: Messages were fetched but contain no text. ` +
          `This is commonly caused by the bot lacking the "Message Content Intent" or missing "Read Message History" permission. ` +
          `Please enable Message Content Intent in the Discord Developer Portal (Bot > Privileged Gateway Intents), ` +
          `re-invite/update the bot permissions in your server, and ensure it has View Channel + Read Message History.`
        );
      }
    } catch {}

    // Full dump of discord content to logs when enabled
    if (logDiscordDump) {
      try {
        const rawSlim = messages.map(m => ({
          id: m.id,
          timestamp: m.timestamp,
          author: m.author ? { id: m.author.id, username: m.author.username, bot: !!m.author.bot } : null,
          content: m.content || '',
          attachments: Array.isArray(m.attachments) ? m.attachments.map(a => ({ id: a.id, filename: a.filename, url: a.url })) : [],
          embeds: Array.isArray(m.embeds) ? m.embeds.map(e => ({ url: e.url || null, title: e.title || null, description: e.description || null })) : [],
        }));
        const dump = {
          channelId,
          window: { startJST: formatJst(startUtc), endJST: formatJst(endUtc) },
          counts: { raw: messages.length, extracted: extracted.length, urls: allUrls.length, texts: textMessages.length },
          extracted,
          raw: rawSlim,
        };
        console.log(`[DISCORD_DUMP][${channelId}]\n` + JSON.stringify(dump, null, 2));
      } catch (e) {
        logWarn(`Failed to dump Discord logs for channel ${channelId}: ${e?.message || e}`);
      }
    }

    // Always show a tiny sample in info logs so that GitHub Actions logs can verify inputs
    try {
      const infoTextSnippets = textMessages
        .map(t => (t || '').replace(/\s+/g, ' ').trim().slice(0, 80))
        .filter(Boolean)
        .slice(0, 3);
      if (infoTextSnippets.length) {
        logInfo(`Sample texts (${infoTextSnippets.length}):`, infoTextSnippets);
      }
      const articleSamples = limitedUrls.slice(0, 3);
      if (articleSamples.length) {
        logInfo(`Sample URLs (${articleSamples.length}):`, articleSamples);
      }
    } catch {}

    const limit = pLimit(maxConcurrency);
    const articles = (await Promise.all(
      limitedUrls.map(u => limit(() => fetchArticleContent(u)))
    )).filter(a => a && (a.title || a.text));

    // Show article sample summaries in info logs (trimmed)
    if (articles.length) {
      const artPreview = articles.slice(0, 3).map(a => ({
        title: (a.title || '').replace(/\s+/g, ' ').trim().slice(0, 60),
        text: (a.text || '').replace(/\s+/g, ' ').trim().slice(0, 100),
        url: a.url
      }));
      logInfo(`Sample articles (${artPreview.length}/${articles.length}):`, artPreview);
    }

    perChannelMaterials.set(channelId, {
      uniqueUrls: limitedUrls,
      articles,
      texts: textMessages,
    });
  }

  // Aggregation: default is single monologue across all channels
  const materialsToInclude = new Map();
  if (aggregationMode === 'per_channel') {
    // Generate one monologue per channel
    for (const [channelId, material] of perChannelMaterials) {
      const script = buildMonologueScript(new Map([[channelId, material]]), { startUtc, endUtc });
      const ttsFiles = await synthesizeMonologueWithMiniMax(script, { channelId });
      const finalFile = await concatAudioFiles(ttsFiles, { channelId });
      await saveRunOutput({ mode: 'per_channel', channelId, startUtc, endUtc, material, minimax: { segments: ttsFiles, finalFile } });
      logInfo(`Monologue audio done for channel ${channelId}`);
    }
    return;
  } else {
    // Single monologue with all materials (from collection channels)
    for (const entry of perChannelMaterials.entries()) {
      materialsToInclude.set(entry[0], entry[1]);
    }
    const script = await buildMonologueScriptAI(materialsToInclude, { startUtc, endUtc });
    const aiTitle = await generatePostTitleAI(materialsToInclude, { startUtc, endUtc });
    const finalFile = await synthesizeLongformMonologue(script, { channelId: 'ALL' });
    await saveRunOutput({ mode: 'single', startUtc, endUtc, title: aiTitle, material: Object.fromEntries(materialsToInclude), minimax: { finalFile } });
    logInfo('Monologue audio done (single request)');

    // Post to Discord (first channel by default)
    if (!noPost && finalFile) {
      const targetChannelId = postingChannelId;
      try {
        const label = `${formatJst(startUtc)} -> ${formatJst(endUtc)}`;
        const ch = await discordGetChannel(targetChannelId).catch(() => null);
        if (ch && ch.type === 15) {
          await postDiscordForumAudio(targetChannelId, finalFile, aiTitle || `本日の自動ラジオ（${label}）`, `本日の自動ラジオ（${label}）`);
          logInfo(`Posted audio as Forum thread to channel ${targetChannelId}`);
        } else {
          await postDiscordAudio(targetChannelId, finalFile, `${aiTitle ? '【' + aiTitle + '】 ' : ''}本日の自動ラジオ（${label}）`);
          logInfo(`Posted audio to Discord text channel ${targetChannelId}`);
        }
      } catch (e) {
        logWarn(`Failed to post audio to Discord: ${e?.message || e}`);
      }
    }
  }
}

async function saveRunOutput(payload) {
  const outDir = path.join(process.cwd(), 'out');
  await fs.mkdir(outDir, { recursive: true });
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = String(now.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(now.getUTCDate()).padStart(2, '0');
  const HH = String(now.getUTCHours()).padStart(2, '0');
  const MM = String(now.getUTCMinutes()).padStart(2, '0');
  const file = path.join(outDir, `run-${yyyy}${mm}${dd}-${HH}${MM}.json`);
  await fs.writeFile(file, JSON.stringify(payload, null, 2), 'utf8');
  logInfo(`Saved run output: ${file}`);
}

// Execute
main().catch(err => {
  logError(err?.stack || err?.message || String(err));
  process.exitCode = 1;
});

// ===== MiniMax integration and audio pipeline =====

function parseDialogueScript(script) {
  // Parse lines like:
  // A: ...
  // B：...
  // 司会A: ... / 相棒B：...
  // Fallback: alternate speakers by line if no prefix
  const lines = (script || '')
    .split(/\r?\n/)
    .map(l => l.trim())
    .filter(Boolean);

  const segments = [];
  let lastSpeaker = 'A';
  for (const raw of lines) {
    let speaker = null;
    let text = raw;
    const m = raw.match(/^((?:司会)?A|(?:相棒)?B)[:：]\s*(.*)$/i);
    if (m) {
      const label = m[1].toUpperCase();
      speaker = label.includes('B') ? 'B' : 'A';
      text = (m[2] || '').trim();
    } else if (/^(A|B)\s+/.test(raw)) {
      const m2 = raw.match(/^(A|B)\s+(.*)$/);
      speaker = m2[1];
      text = (m2[2] || '').trim();
    }

    if (!speaker) {
      // No explicit label → alternate
      speaker = lastSpeaker === 'A' ? 'B' : 'A';
    }
    lastSpeaker = speaker;
    if (text) segments.push({ speaker, text });
  }
  return segments;
}

async function minimaxT2ARequest(text, voiceId, idx) {
  // MiniMax T2A v2 (non-streaming). API per docs: https://api.minimax.io/v1/t2a_v2?GroupId=${group_id}
  const url = `${minimaxEndpoint}?GroupId=${encodeURIComponent(minimaxGroupId)}`;
  const body = {
    model: minimaxModel,
    text,
    stream: false,
    voice_setting: {
      voice_id: voiceId,
      speed: minimaxSpeed,
      vol: 1,
      pitch: 0,
    },
    audio_setting: {
      sample_rate: 32000,
      bitrate: 128000,
      format: minimaxAudioFormat,
      channel: 1,
    },
    output_format: 'hex',
    language_boost: 'Japanese',
  };

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${minimaxApiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });

  if (res.status === 429) {
    const retryAfter = Number(res.headers.get('retry-after') || '1');
    logWarn(`MiniMax rate limited. Retrying after ${retryAfter}s`);
    await delay(retryAfter * 1000);
    return minimaxT2ARequest(text, voiceId, idx);
  }

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`MiniMax API error ${res.status}: ${txt}`);
  }

  const ct = res.headers.get('content-type') || '';
  if (ct.includes('audio')) {
    const buf = Buffer.from(await res.arrayBuffer());
    return buf;
  }
  // Try JSON (non-streaming)
  const data = await res.json().catch(() => null);
  // Success pattern per docs: { data: { audio: 'hex audio', status: 2, ... }, base_resp: { status_code: 0, ... } }
  if (data && data.base_resp && data.base_resp.status_code !== 0) {
    const code = data.base_resp.status_code;
    const msg = data.base_resp.status_msg || '';
    throw new Error(`MiniMax API error status_code=${code} ${msg}`);
  }
  // audio_hex
  const hexAudio = data && data.data && typeof data.data.audio === 'string' ? data.data.audio : null;
  if (hexAudio) {
    // 'hex audio' => binary bytes
    return Buffer.from(hexAudio, 'hex');
  }
  // Fallbacks: sometimes variants exist
  const audioUrl = data && typeof data.audio_url === 'string' ? data.audio_url : null;
  if (audioUrl) {
    const ares = await fetch(audioUrl);
    if (!ares.ok) throw new Error(`MiniMax audio_url fetch failed ${ares.status}`);
    return Buffer.from(await ares.arrayBuffer());
  }
  const b64Candidates = [];
  if (data) {
    if (typeof data.audio_base64 === 'string') b64Candidates.push(data.audio_base64);
    if (data.data && typeof data.data.audio_base64 === 'string') b64Candidates.push(data.data.audio_base64);
    if (Array.isArray(data.data) && data.data[0] && typeof data.data[0].audio_base64 === 'string') b64Candidates.push(data.data[0].audio_base64);
  }
  if (b64Candidates.length > 0) {
    return Buffer.from(b64Candidates[0], 'base64');
  }
  const msg = data && (data.message || data.msg || data.error || JSON.stringify(data).slice(0, 300));
  throw new Error(`MiniMax API returned unexpected response format${msg ? `: ${msg}` : ''}`);
}

async function synthesizeLongformMonologue(script, { channelId }) {
  if (!script || !script.trim()) {
    logWarn('No monologue text to synthesize');
    return null;
  }
  const outDir = path.join(process.cwd(), 'out');
  await fs.mkdir(outDir, { recursive: true });
  const buf = await minimaxT2ARequest(script, minimaxVoiceSingle, 0);
  const ts = Date.now();
  const outFile = path.join(outDir, `episode-${channelId}-${ts}.${minimaxAudioFormat}`);
  await fs.writeFile(outFile, buf);
  return outFile;
}

function splitTextIntoChunks(text, chunkSize) {
  const arr = [];
  let i = 0;
  while (i < text.length) {
    arr.push(text.slice(i, i + chunkSize));
    i += chunkSize;
  }
  return arr;
}

// ffmpeg concat removed (single request synthesis)


// ===== Discord upload helpers =====
async function postDiscordAudio(channelId, filePath, content) {
  // Discord requires multipart/form-data with files[0] and payload_json
  const filename = path.basename(filePath);
  const form = new FormData();
  // undici(FormData) requires Blob/File, not Node streams
  const fileBuf = await fs.readFile(filePath);
  const blob = new Blob([fileBuf]);
  form.append('files[0]', blob, filename);
  const payload = {
    content: content || '',
    allowed_mentions: { parse: [] },
  };
  form.append('payload_json', JSON.stringify(payload));

  const url = `https://discord.com/api/v10/channels/${channelId}/messages`;
  while (true) {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Bot ${discordBotToken}`,
      },
      body: form,
    });
    if (res.status === 429) {
      const retryAfter = Number(res.headers.get('retry-after') || '1');
      logWarn(`Discord rate limited on upload. Retrying after ${retryAfter}s`);
      await delay(retryAfter * 1000);
      continue;
    }
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`Discord upload error ${res.status}: ${txt}`);
    }
    return await res.json();
  }
}

// Forum (GuildForum) posting helper (type 15). Creates a thread with first message containing the file
async function postDiscordForumAudio(forumChannelId, filePath, title, content) {
  const filename = path.basename(filePath);
  const form = new FormData();
  // attachments metadata must reference the file indices
  const payload = {
    name: title || filename,
    message: {
      content: content || '',
      allowed_mentions: { parse: [] },
      attachments: [
        {
          id: 0,
          filename
        }
      ]
    },
  };
  const tagIds = discordForumTagIdsEnv
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  if (tagIds.length > 0) payload.applied_tags = tagIds;

  const fileBuf = await fs.readFile(filePath);
  const blob = new Blob([fileBuf]);
  form.append('files[0]', blob, filename);
  form.append('payload_json', JSON.stringify(payload));

  const url = `https://discord.com/api/v10/channels/${forumChannelId}/threads`;
  while (true) {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Bot ${discordBotToken}`,
      },
      body: form,
    });
    if (res.status === 429) {
      const retryAfter = Number(res.headers.get('retry-after') || '1');
      logWarn(`Discord rate limited on forum upload. Retrying after ${retryAfter}s`);
      await delay(retryAfter * 1000);
      continue;
    }
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`Discord forum upload error ${res.status}: ${txt}`);
    }
    return await res.json();
  }
}

// ===== Utility to post latest audio from out/ for post-only mode =====
async function postLatestAudioFromOut(startUtc, endUtc) {
  const outDir = path.join(process.cwd(), 'out');
  try {
    const files = await fs.readdir(outDir);
    // try to capture latest run json for title
    const runJson = files
      .filter(f => /^run-\d{8}-\d{4}\.json$/.test(f))
      .map(f => ({ f, t: Number((f.match(/run-(\d{8})-(\d{4})\.json/))?.slice(1).join('') || '0') }))
      .sort((a, b) => b.t - a.t)[0];
    let savedTitle = null;
    if (runJson) {
      try {
        const js = JSON.parse(await fs.readFile(path.join(outDir, runJson.f), 'utf8'));
        savedTitle = js?.title || null;
      } catch {}
    }
    const audio = files
      .filter(f => /\.(mp3|wav|ogg|m4a)$/.test(f))
      .map(f => ({ f, t: Number((f.match(/-(\d+)\./)?.[1] || '0')) }))
      .sort((a, b) => b.t - a.t)[0];
    if (!audio) {
      logWarn('No audio file found in out/ for post-only mode');
      return;
    }
    const filePath = path.join(outDir, audio.f);
    const label = `${formatJst(startUtc)} -> ${formatJst(endUtc)}`;
    const targetChannelId = (process.env.DISCORD_CHANNEL_IDS || '').split(',').map(s => s.trim()).filter(Boolean)[0];
    if (!targetChannelId) throw new Error('No DISCORD_CHANNEL_IDS provided');
    const ch = await discordGetChannel(targetChannelId).catch(() => null);
    if (ch && ch.type === 15) {
      await postDiscordForumAudio(targetChannelId, filePath, savedTitle || `本日の自動ラジオ（${label}）`, `本日の自動ラジオ（${label}）`);
      logInfo(`Posted latest audio as Forum thread to channel ${targetChannelId}`);
    } else {
      await postDiscordAudio(targetChannelId, filePath, `${savedTitle ? '【' + savedTitle + '】 ' : ''}本日の自動ラジオ（${label}）`);
      logInfo(`Posted latest audio to text channel ${targetChannelId}`);
    }
  } catch (e) {
    logWarn(`post-only failed: ${e?.message || e}`);
  }
}
