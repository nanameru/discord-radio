#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { JSDOM } from 'jsdom';
import { Readability } from '@mozilla/readability';
import pLimit from 'p-limit';

// Configuration via environment variables
const discordBotToken = process.env.DISCORD_BOT_TOKEN;
const discordChannelIdsEnv = process.env.DISCORD_CHANNEL_IDS || '';
const castmakeApiKey = process.env.CASTMAKE_API_KEY;
const castmakeChannelId = process.env.CASTMAKE_CHANNEL_ID;
const aggregationMode = process.env.AGGREGATION_MODE || 'single'; // 'single' | 'per_channel' (single default)
const maxArticleCount = parseInt(process.env.MAX_URLS || '20', 10);
const maxArticleChars = parseInt(process.env.MAX_TEXT_CHARS || '2000', 10);
const maxConcurrency = parseInt(process.env.MAX_CONCURRENCY || '4', 10);
const logLevel = process.env.LOG_LEVEL || 'info';

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
  if (!castmakeApiKey) missing.push('CASTMAKE_API_KEY');
  if (!castmakeChannelId) missing.push('CASTMAKE_CHANNEL_ID');
  if (!discordChannelIdsEnv) missing.push('DISCORD_CHANNEL_IDS');
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

function buildConversationText({
  startUtc,
  endUtc,
  perChannelMaterials,
}) {
  const startJstLabel = formatJst(startUtc);
  const endJstLabel = formatJst(endUtc);

  const lines = [];
  lines.push(`期間: ${startJstLabel} 〜 ${endJstLabel}`);
  lines.push('概要: 以下のDiscordチャンネルで共有された話題と記事をもとに、2人の会話（日本語）でラジオ台本を作成してください。');
  lines.push('要件: 7〜12分程度、適度な雑談、初心者にも分かりやすく要点を解説。重要な箇所では出典URLに言及してください。');
  lines.push('登場人物: 司会Aと相棒B（どちらも落ち着いたトーン）。');
  lines.push('トピック:');

  let topicIndex = 1;
  for (const [channelId, material] of perChannelMaterials) {
    const { uniqueUrls, articles, miscMessages } = material;
    if (perChannelMaterials.size > 1) {
      lines.push(`- チャンネル ${channelId}:`);
    }
    for (const art of articles) {
      const title = art.title || '無題の記事';
      const excerpt = art.text ? art.text.slice(0, 400) : '';
      lines.push(`${topicIndex}. ${title}`);
      if (excerpt) lines.push(`   概要: ${excerpt}`);
      lines.push(`   URL: ${art.url}`);
      topicIndex += 1;
    }
    if (miscMessages.length) {
      const sample = miscMessages.slice(0, 3).map(m => `「${m.slice(0, 120)}」`).join(' / ');
      lines.push(`   その他の話題: ${sample}`);
    }
  }

  lines.push('出力形式: 会話台本のみを出力してください。セクション見出しは不要です。');
  return lines.join('\n');
}

async function callCastmakeConversation({ text }) {
  const url = 'https://api.castmake-ai.com/v1/episodes_conversation';
  const body = {
    channelId: castmakeChannelId,
    text,
  };
  while (true) {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'x-castmake-api-key': castmakeApiKey,
        'content-type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    if (res.status === 429) {
      const retryAfterHeader = res.headers.get('retry-after');
      const retryAfter = Number(retryAfterHeader || '1');
      logWarn(`Castmake rate limited. Retrying after ${retryAfter}s`);
      await delay(retryAfter * 1000);
      continue;
    }
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Castmake API error ${res.status}: ${text}`);
    }
    return res.json();
  }
}

async function main() {
  assertRequiredEnv();
  const { date } = parseCliArgs();
  const endUtc = date ? buildJstEndUtcFromDateString(date) : getDefaultJstEndUtcNow();
  const startUtc = new Date(endUtc.getTime() - 24 * 60 * 60 * 1000);

  logInfo(`Time window (JST): ${formatJst(startUtc)} -> ${formatJst(endUtc)}`);

  const discordChannelIds = discordChannelIdsEnv
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);

  if (discordChannelIds.length === 0) {
    throw new Error('No DISCORD_CHANNEL_IDS provided');
  }

  const perChannelMaterials = new Map();
  for (const channelId of discordChannelIds) {
    logInfo(`Fetching Discord messages for channel ${channelId}...`);
    const messages = await fetchChannelMessagesInRange(channelId, startUtc, endUtc);
    logInfo(`Fetched ${messages.length} messages in range for channel ${channelId}`);

    const extracted = filterAndExtractFromMessages(messages);
    const allUrls = Array.from(new Set(extracted.flatMap(m => m.urls)));
    const limitedUrls = allUrls.slice(0, maxArticleCount);

    const nonUrlMessages = extracted
      .map(m => m.content)
      .filter(Boolean);

    const limit = pLimit(maxConcurrency);
    const articles = (await Promise.all(
      limitedUrls.map(u => limit(() => fetchArticleContent(u)))
    )).filter(a => a && (a.title || a.text));

    perChannelMaterials.set(channelId, {
      uniqueUrls: limitedUrls,
      articles,
      miscMessages: nonUrlMessages,
    });
  }

  // Aggregation: default is single conversation across all channels
  const materialsToInclude = new Map();
  if (aggregationMode === 'per_channel') {
    // Generate one conversation per channel
    for (const [channelId, material] of perChannelMaterials) {
      const text = buildConversationText({ startUtc, endUtc, perChannelMaterials: new Map([[channelId, material]]) });
      logInfo(`Calling Castmake conversation for channel ${channelId}...`);
      const resp = await callCastmakeConversation({ text });
      await saveRunOutput({ mode: 'per_channel', channelId, startUtc, endUtc, material, castmakeResponse: resp });
      logInfo(`Castmake done for channel ${channelId}: episodeId=${resp.episodeId}`);
    }
    return;
  } else {
    // Single conversation with all materials
    for (const entry of perChannelMaterials.entries()) {
      materialsToInclude.set(entry[0], entry[1]);
    }
    const text = buildConversationText({ startUtc, endUtc, perChannelMaterials: materialsToInclude });
    logInfo('Calling Castmake conversation (single aggregated)...');
    const resp = await callCastmakeConversation({ text });
    await saveRunOutput({ mode: 'single', startUtc, endUtc, material: Object.fromEntries(materialsToInclude), castmakeResponse: resp });
    logInfo(`Castmake done: episodeId=${resp.episodeId}`);
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


