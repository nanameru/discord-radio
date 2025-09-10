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

function buildMonologueScriptHeuristic(perChannelMaterials, { startUtc, endUtc }) {
  const startJstLabel = formatJst(startUtc);
  const endJstLabel = formatJst(endUtc);
  const bullets = [];
  for (const [, material] of perChannelMaterials) {
    for (const art of material.articles) {
      const title = art.title || '無題の記事';
      const summary = art.text ? art.text.slice(0, 240).replace(/\s+/g, ' ') : '';
      bullets.push({ title, url: art.url, summary });
    }
  }

  const lines = [];
  lines.push(`こんにちは。本放送では、${startJstLabel} から ${endJstLabel} の間に共有された話題をまとめてご紹介します。`);
  if (bullets.length === 0) {
    lines.push('本日は特筆すべき新着トピックはありませんでした。最近の動向の振り返りや、今後の見どころを簡単にお話しします。');
  } else {
    lines.push('まずは注目のトピックから。');
  }
  let idx = 1;
  for (const b of bullets) {
    lines.push(`${idx}. ${b.title}`);
    if (b.summary) lines.push(`概要: ${b.summary}`);
    lines.push(`出典: ${b.url}`);
    idx += 1;
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
  for (const [, material] of perChannelMaterials) {
    for (const art of material.articles) {
      topics.push({ title: art.title || '無題の記事', url: art.url, summary: art.text ? art.text.slice(0, 800) : '' });
    }
  }

  const system = 'あなたは日本語のナレーターです。ポッドキャスト用の落ち着いた独白台本を作成します。専門用語は平易に説明し、冗長さは避け、5〜9分程度の長さを目安にしてください。';
  const user = `期間: ${startJstLabel} 〜 ${endJstLabel}\n\n話題一覧（title/url/summaryの順）:\n${topics.map(t => `- ${t.title}\n  ${t.url}\n  ${t.summary}`).join('\n')}\n\n要件:\n- 導入→各トピックの要点→締めの順に\n- 適宜「出典: URL」で参照提示\n- 台本のみを出力（ヘッダ不要）`;

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
    // Single monologue with all materials
    for (const entry of perChannelMaterials.entries()) {
      materialsToInclude.set(entry[0], entry[1]);
    }
    const script = await buildMonologueScriptAI(materialsToInclude, { startUtc, endUtc });
    const finalFile = await synthesizeLongformMonologue(script, { channelId: 'ALL' });
    await saveRunOutput({ mode: 'single', startUtc, endUtc, material: Object.fromEntries(materialsToInclude), minimax: { finalFile } });
    logInfo('Monologue audio done (single request)');

    // Post to Discord (first channel by default)
    if (finalFile) {
      const targetChannelId = discordChannelIds[0];
      try {
        const label = `${formatJst(startUtc)} -> ${formatJst(endUtc)}`;
        const ch = await discordGetChannel(targetChannelId).catch(() => null);
        if (ch && ch.type === 15) {
          await postDiscordForumAudio(targetChannelId, finalFile, `本日の自動ラジオ（${label}）`, `本日の自動ラジオ（${label}）`);
          logInfo(`Posted audio as Forum thread to channel ${targetChannelId}`);
        } else {
          await postDiscordAudio(targetChannelId, finalFile, `本日の自動ラジオ（${label}）`);
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
  const stream = createReadStream(filePath);
  form.append('files[0]', stream, filename);
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
  const stream = createReadStream(filePath);
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

  form.append('files[0]', stream, filename);
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

