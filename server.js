const express = require('express');
const fetch = require('node-fetch');
const xml2js = require('xml2js');
const cron = require('node-cron');
const cors = require('cors');
const path = require('path');
const { translate } = require('google-translate-api-x');

const app = express();
const PORT = process.env.PORT || 3000;
const parser = new xml2js.Parser({
  explicitArray: false,
  trim: true,
  normalizeTags: false,
});

const RSS_SOURCES = [
  { name: 'Times of Israel', url: 'https://www.timesofisrael.com/feed/', lang: 'en', region: 'Israel', weight: 10 },
  { name: 'Jerusalem Post', url: 'https://www.jpost.com/rss', lang: 'en', region: 'Israel', weight: 10 },
  { name: 'i24NEWS', url: 'https://www.i24news.tv/en/rss', lang: 'en', region: 'Israel', weight: 8 },
  { name: 'JTA', url: 'https://www.jta.org/feed/', lang: 'en', region: 'Global', weight: 7 },
  { name: 'The Forward', url: 'https://forward.com/feed/', lang: 'en', region: 'USA', weight: 7 },
  { name: 'Algemeiner', url: 'https://www.algemeiner.com/feed/', lang: 'en', region: 'USA', weight: 6 },
  { name: 'Israel National News', url: 'https://www.israelnationalnews.com/rss.aspx', lang: 'en', region: 'Israel', weight: 7 },
  { name: 'Jewish News Syndicate', url: 'https://www.jns.org/feed/', lang: 'en', region: 'USA', weight: 6 },
  { name: 'Ynet (עברית)', url: 'https://www.ynet.co.il/Integration/StoryRss1854.xml', lang: 'he', region: 'Israel', weight: 9 },
  { name: 'Walla (עברית)', url: 'https://rss.walla.co.il/feed/1?type=main', lang: 'he', region: 'Israel', weight: 8 },
  { name: 'Israel Hayom', url: 'https://www.israelhayom.com/feed/', lang: 'en', region: 'Israel', weight: 8 },
  { name: 'Google News Israel', url: 'https://news.google.com/rss/search?q=Israel%20OR%20Israeli%20OR%20Jewish%20OR%20Jews%20when:1d&hl=en-US&gl=US&ceid=US:en', lang: 'en', region: 'Global', weight: 8 },
  { name: 'BBC Middle East', url: 'https://feeds.bbci.co.uk/news/world/middle_east/rss.xml', lang: 'en', region: 'Global', weight: 6 },
  { name: 'NYT Middle East', url: 'https://rss.nytimes.com/services/xml/rss/nyt/MiddleEast.xml', lang: 'en', region: 'Global', weight: 6 },
];

const TRANSLATE_TARGET = 'zh-CN';
const TRANSLATE_CONCURRENCY = 2;
const TRANSLATE_BATCH_DELAY_MS = 200;
const TRANSLATE_TIMEOUT_MS = 8000;
const TRANSLATE_LIMIT = 25;
const MAX_AGE_MS = 24 * 60 * 60 * 1000;
const FUTURE_SKEW_MS = 60 * 60 * 1000;
const ITEMS_PER_SOURCE = 30;
const RSS_TIMEOUT_MS = 12000;
const UPDATE_LOCK_MAX_MS = 3 * 60 * 1000;
const APP_VERSION = '2.2.0';

const ALWAYS_RELEVANT_SOURCES = new Set([
  'Times of Israel',
  'Jerusalem Post',
  'i24NEWS',
  'JTA',
  'The Forward',
  'Algemeiner',
  'Israel National News',
  'Jewish News Syndicate',
  'Israel Hayom',
  'Google News Israel',
]);

const RELEVANCE_KEYWORDS = [
  'israel', 'israeli', 'jerusalem', 'tel aviv', 'tel-aviv', 'gaza', 'hamas', 'hezbollah',
  'jewish', 'jews', 'judaism', 'jewry', 'antisemit', 'anti-semit', 'synagogue', 'idf',
  'netanyahu', 'knesset', 'zionist', 'zionism', 'holocaust', 'hebrew', 'kosher',
  'west bank', 'judea', 'samaria', 'settler', 'mossad', 'shin bet', 'kibbutz',
  'palestine', 'palestinian', 'rafah', 'hostages', 'october 7', 'oct. 7', 'oct 7',
  'ben gvir', 'smotrich', 'gallant', 'golan', 'haifa', 'ashkelon', 'beersheba',
  'ישראל', 'ישראלי', 'ישראלים', 'ירושלים', 'תל אביב', 'תל-אביב', 'עזה', 'חמאס', 'חיזבאללה',
  'יהודי', 'יהודים', 'יהדות', 'אנטישמי', 'צה״ל', 'צה"ל', 'צהל', 'נתניהו', 'כנסת', 'הכנסת',
  'הגדה', 'רצועת', 'רצועה', 'חטוף', 'חטופים', 'ממשלה', 'הממשלה', 'קבינט', 'הקבינט',
  'ראש הממשלה', 'ביטחון', 'חייל', 'חיילים', 'רקטה', 'יירוט', 'מתנחל', 'התנחלות',
  'איראן', 'לבנון', 'סוריה', 'גולן', 'חיפה', 'אשקלון', 'באר שבע', 'קיבוץ', 'מושב',
  'בן גביר', 'סמוטריץ', 'גנץ', 'לפיד', 'בנט',
  'израил', 'израиль', 'евре', 'иерусалим', 'тель-авив', 'газа', 'хамас', 'антисемит',
  'israël', 'juif', 'juive', 'juifs', 'jérusalem', 'antisémit', 'hébreu',
];

let newsCache = {
  data: [],
  translated: {},
  lastUpdate: null,
  updating: false,
  updateStartedAt: null,
  lastError: null,
};

function withTimeout(promise, ms, label = 'operation') {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function isWithin24Hours(pubDate) {
  const t = new Date(pubDate).getTime();
  if (Number.isNaN(t)) return false;
  const age = Date.now() - t;
  return age <= MAX_AGE_MS && age >= -FUTURE_SKEW_MS;
}

function isIsraelOrJewishRelated(item) {
  if (ALWAYS_RELEVANT_SOURCES.has(item.source)) return true;
  const text = `${item.title || ''} ${item.description || ''}`.toLowerCase();
  return RELEVANCE_KEYWORDS.some((keyword) => text.includes(keyword.toLowerCase()));
}

function extractText(value) {
  if (!value) return '';
  if (typeof value === 'string') return value;
  if (typeof value === 'object') return value._ || value['#text'] || value.href || '';
  return String(value);
}

function extractLink(item) {
  let link = item.link || item.id || item.guid || '';
  if (Array.isArray(link)) link = link[0];
  if (typeof link === 'object') link = link.href || link._ || link['#text'] || '';
  if (typeof link === 'object' && link._) link = link._;
  return String(link || '').trim();
}

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

async function fetchRSS(source) {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), RSS_TIMEOUT_MS);
    const res = await fetch(source.url, {
      signal: controller.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; JewishIsraelNews/2.2; +https://jewish-israel-news.onrender.com)',
        Accept: 'application/rss+xml, application/xml, text/xml, */*',
        'Accept-Language': 'en-US,en;q=0.9,he;q=0.8',
      },
      redirect: 'follow',
    });
    clearTimeout(timeout);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    let xml = await res.text();
    // Some feeds ship BOM / bare ampersands that break XML parsers.
    xml = xml.replace(/^\uFEFF/, '').replace(/&(?!(?:#\d+|#x[\da-fA-F]+|[a-zA-Z]+);)/g, '&amp;');
    let result;
    try {
      result = await parser.parseStringPromise(xml);
    } catch (parseErr) {
      // Retry with a more lenient parser for messy feeds.
      const loose = new xml2js.Parser({ explicitArray: false, strict: false, normalizeTags: true });
      result = await loose.parseStringPromise(xml);
    }
    const channel = result.rss?.channel || result.feed || result['rdf:RDF']
      || result.RSS?.channel || result.FEED;
    const rawItems = channel?.item || channel?.entry || channel?.ITEM || channel?.ENTRY
      || result.item || [];
    const items = Array.isArray(rawItems) ? rawItems : [rawItems];
    return items.slice(0, ITEMS_PER_SOURCE).map((item, idx) => {
      const title = extractText(item.title).trim();
      const link = extractLink(item);
      let desc = extractText(item.description || item.summary || item.content || item['content:encoded']);
      desc = desc.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
      if (desc.length > 300) desc = desc.substring(0, 300) + '...';
      const pubDate = extractText(item.pubDate || item.published || item.updated || item['dc:date'] || '');
      return {
        id: `${source.name}_${idx}_${Date.now()}`,
        title,
        link,
        description: desc,
        pubDate,
        source: source.name,
        lang: source.lang,
        region: source.region,
        weight: source.weight,
      };
    }).filter((item) => item.title && item.link && isWithin24Hours(item.pubDate));
  } catch (e) {
    console.error(`[ERROR] ${source.name}: ${e.message}`);
    return [];
  }
}

async function translateText(text, lang, retries = 1) {
  if (!text || text.length < 2) return '';
  const cacheKey = `${lang}_${text.substring(0, 100)}`;
  if (newsCache.translated[cacheKey]) return newsCache.translated[cacheKey];

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const result = await withTimeout(
        translate(text.substring(0, 400), {
          from: lang === 'he' ? 'iw' : lang,
          to: TRANSLATE_TARGET,
          forceFrom: false,
          forceTo: true,
        }),
        TRANSLATE_TIMEOUT_MS,
        'translate'
      );
      if (result?.text) {
        newsCache.translated[cacheKey] = result.text;
        return result.text;
      }
    } catch (e) {
      if (attempt === retries) {
        console.warn(`[TRANSLATE ERROR] ${lang}: ${e.message}`);
      } else {
        await new Promise((r) => setTimeout(r, 400 * (attempt + 1)));
      }
    }
  }
  return '';
}

async function processTranslations(items) {
  const toTranslate = items.filter((item) => item.lang !== 'zh');
  for (let i = 0; i < toTranslate.length; i += TRANSLATE_CONCURRENCY) {
    const batch = toTranslate.slice(i, i + TRANSLATE_CONCURRENCY);
    await Promise.all(batch.map(async (item) => {
      const translated = await translateText(item.title, item.lang);
      if (translated) item.translation = translated;
    }));
    if (i + TRANSLATE_CONCURRENCY < toTranslate.length) {
      await new Promise((r) => setTimeout(r, TRANSLATE_BATCH_DELAY_MS));
    }
  }
}

function isUpdateLockStuck() {
  if (!newsCache.updating) return false;
  if (!newsCache.updateStartedAt) return true;
  return Date.now() - newsCache.updateStartedAt > UPDATE_LOCK_MAX_MS;
}

async function updateNews({ force = false } = {}) {
  if (newsCache.updating) {
    if (!force && !isUpdateLockStuck()) {
      console.log('[UPDATE] 已有更新在进行，跳过');
      return { started: false, reason: 'busy' };
    }
    console.warn('[UPDATE] 检测到卡死的更新锁，强制重新开始');
  }

  newsCache.updating = true;
  newsCache.updateStartedAt = Date.now();
  newsCache.lastError = null;
  console.log(`[${new Date().toISOString()}] 开始更新新闻...`);

  try {
    const collected = [];
    const batchSize = 4;
    for (let i = 0; i < RSS_SOURCES.length; i += batchSize) {
      const batch = RSS_SOURCES.slice(i, i + batchSize);
      const results = await Promise.all(batch.map((s) => fetchRSS(s)));
      results.forEach((items) => collected.push(...items));
      console.log(`[PROGRESS] 已获取过去24小时内 ${collected.length} 条`);
    }

    const allNews = collected.filter(isIsraelOrJewishRelated);
    const dropped = collected.length - allNews.length;
    if (dropped > 0) {
      console.log(`[FILTER] 过滤掉 ${dropped} 条与以色列/犹太无关的新闻`);
    }

    allNews.sort((a, b) => {
      try {
        const timeDiff = new Date(b.pubDate) - new Date(a.pubDate);
        if (Math.abs(timeDiff) < 3600000) return (b.weight || 0) - (a.weight || 0);
        return timeDiff;
      } catch (e) {
        return 0;
      }
    });

    // Publish immediately so the site is usable even if translation is slow/blocked.
    newsCache.data = allNews;
    newsCache.lastUpdate = new Date().toISOString();
    console.log(`[PUBLISH] 已发布 ${allNews.length} 条，开始翻译前 ${Math.min(TRANSLATE_LIMIT, allNews.length)} 条...`);

    try {
      await withTimeout(
        processTranslations(allNews.slice(0, TRANSLATE_LIMIT)),
        TRANSLATE_LIMIT * TRANSLATE_TIMEOUT_MS,
        'batch-translate'
      );
    } catch (e) {
      console.warn(`[TRANSLATE BATCH] ${e.message}`);
    }

    // Refresh cache reference so clients see translations.
    newsCache.data = [...allNews];
    newsCache.lastUpdate = new Date().toISOString();
    console.log(`[DONE] 共 ${allNews.length} 条相关新闻（过去24小时），更新完成`);
    return { started: true, total: allNews.length };
  } catch (e) {
    newsCache.lastError = e.message;
    console.error(`[UPDATE FATAL] ${e.message}`);
    return { started: true, error: e.message };
  } finally {
    newsCache.updating = false;
    newsCache.updateStartedAt = null;
  }
}

app.get('/api/news', (req, res) => {
  const { lang, limit = 100, offset = 0 } = req.query;
  let data = newsCache.data;
  if (lang && lang !== 'all') data = data.filter((n) => n.lang === lang);
  const total = data.length;
  data = data.slice(parseInt(offset, 10), parseInt(offset, 10) + parseInt(limit, 10));
  res.json({
    success: true,
    total,
    lastUpdate: newsCache.lastUpdate,
    updating: newsCache.updating,
    lastError: newsCache.lastError,
    data,
  });
});

app.get('/api/refresh', async (req, res) => {
  const force = String(req.query.force || '') === '1' || isUpdateLockStuck();
  if (newsCache.updating && !force) {
    return res.json({
      success: false,
      message: '更新中，请稍候',
      updating: true,
      startedAt: newsCache.updateStartedAt,
    });
  }
  updateNews({ force }).catch(console.error);
  res.json({ success: true, message: force ? '已强制重新更新' : '已触发更新', forced: force });
});

app.get('/api/sources', (req, res) => {
  res.json({
    success: true,
    sources: RSS_SOURCES.map((s) => ({ name: s.name, lang: s.lang, region: s.region })),
  });
});

app.get('/api/health', (req, res) => {
  const stuck = isUpdateLockStuck();
  res.json({
    success: true,
    ok: !stuck,
    version: APP_VERSION,
    total: newsCache.data.length,
    lastUpdate: newsCache.lastUpdate,
    updating: newsCache.updating,
    stuck,
    lastError: newsCache.lastError,
  });
});

app.get('/api/version', (req, res) => {
  res.json({
    success: true,
    version: APP_VERSION,
    translator: 'google-translate-api-x',
    filters: { maxAgeHours: 24, israelJewishOnly: true },
  });
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

cron.schedule('*/5 * * * *', () => {
  console.log('[CRON] 触发定时更新');
  updateNews({ force: isUpdateLockStuck() }).catch(console.error);
});

// Safety valve: if a lock somehow survives, clear it.
setInterval(() => {
  if (isUpdateLockStuck()) {
    console.warn('[WATCHDOG] 清除卡死的更新锁');
    newsCache.updating = false;
    newsCache.updateStartedAt = null;
    newsCache.lastError = newsCache.lastError || 'update lock cleared by watchdog';
  }
}, 30000);

updateNews().catch(console.error);

app.listen(PORT, () => {
  console.log(`✡ 犹太社区与以色列新闻聚合器`);
  console.log(`🌐 服务器运行在 http://localhost:${PORT}`);
  console.log(`📰 监控 ${RSS_SOURCES.length} 个新闻源`);
  console.log(`⏰ 每5分钟自动更新`);
});
