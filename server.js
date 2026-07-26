const express = require('express');
const fetch = require('node-fetch');
const xml2js = require('xml2js');
const cron = require('node-cron');
const cors = require('cors');
const path = require('path');
const { translate } = require('google-translate-api-x');

const app = express();
const PORT = process.env.PORT || 3000;
const parser = new xml2js.Parser({ explicitArray: false });

const RSS_SOURCES = [
  { name: 'Times of Israel', url: 'https://www.timesofisrael.com/feed/', lang: 'en', region: 'Israel', weight: 10 },
  { name: 'Jerusalem Post', url: 'https://www.jpost.com/rss', lang: 'en', region: 'Israel', weight: 10 },
  { name: 'Haaretz', url: 'https://www.haaretz.com/rss', lang: 'en', region: 'Israel', weight: 10 },
  { name: 'Ynetnews', url: 'https://www.ynetnews.com/category/3082/rss', lang: 'en', region: 'Israel', weight: 8 },
  { name: 'i24NEWS', url: 'https://www.i24news.tv/en/rss', lang: 'en', region: 'Israel', weight: 8 },
  { name: 'JTA', url: 'https://www.jta.org/feed/', lang: 'en', region: 'Global', weight: 7 },
  { name: 'The Forward', url: 'https://forward.com/feed/', lang: 'en', region: 'USA', weight: 7 },
  { name: 'Algemeiner', url: 'https://www.algemeiner.com/feed/', lang: 'en', region: 'USA', weight: 6 },
  { name: 'Israel National News', url: 'https://www.israelnationalnews.com/rss.aspx', lang: 'en', region: 'Israel', weight: 7 },
  { name: 'Jewish News Syndicate', url: 'https://www.jns.org/feed/', lang: 'en', region: 'USA', weight: 6 },
  { name: 'Ynet (עברית)', url: 'https://www.ynet.co.il/Integration/StoryRss1854.xml', lang: 'he', region: 'Israel', weight: 9 },
  { name: 'Walla (עברית)', url: 'https://rss.walla.co.il/feed/1?type=main', lang: 'he', region: 'Israel', weight: 8 },
  { name: 'Israel Hayom (עברית)', url: 'https://www.israelhayom.com/feed/', lang: 'he', region: 'Israel', weight: 8 },
  { name: 'Maariv (עברית)', url: 'https://www.maariv.co.il/rss', lang: 'he', region: 'Israel', weight: 7 },
  { name: '9 TV (Русский)', url: 'https://www.9tv.co.il/rss', lang: 'ru', region: 'Israel', weight: 6 },
  { name: 'Vesty (Русский)', url: 'https://www.vesty.co.il/main/rss', lang: 'ru', region: 'Israel', weight: 6 },
  { name: 'Alliance (Français)', url: 'https://www.alliancemagazine.org/feed/', lang: 'fr', region: 'France', weight: 5 },
];

const TRANSLATE_TARGET = 'zh-CN';
const TRANSLATE_CONCURRENCY = 3;
const TRANSLATE_BATCH_DELAY_MS = 300;
const MAX_AGE_MS = 24 * 60 * 60 * 1000;
const FUTURE_SKEW_MS = 60 * 60 * 1000;
const ITEMS_PER_SOURCE = 30;

// Dedicated Israel / Jewish news outlets: keep all stories.
// General portals (Walla/Ynet/Maariv/Alliance) must match relevance keywords.
const ALWAYS_RELEVANT_SOURCES = new Set([
  'Times of Israel',
  'Jerusalem Post',
  'Haaretz',
  'Ynetnews',
  'i24NEWS',
  'JTA',
  'The Forward',
  'Algemeiner',
  'Israel National News',
  'Jewish News Syndicate',
  'Israel Hayom (עברית)',
  '9 TV (Русский)',
  'Vesty (Русский)',
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

let newsCache = { data: [], translated: {}, lastUpdate: null, updating: false };

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

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

async function fetchRSS(source) {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20000);
    const res = await fetch(source.url, {
      signal: controller.signal,
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
    });
    clearTimeout(timeout);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const xml = await res.text();
    const result = await parser.parseStringPromise(xml);
    const channel = result.rss?.channel || result.feed;
    const rawItems = channel?.item || channel?.entry || [];
    const items = Array.isArray(rawItems) ? rawItems : [rawItems];
    return items.slice(0, ITEMS_PER_SOURCE).map((item, idx) => {
      let title = typeof item.title === 'string' ? item.title : (item.title?._ || '');
      let link = typeof item.link === 'string' ? item.link : (item.link?.href || '');
      if (Array.isArray(item.link)) link = item.link[0]?.href || item.link[0] || '';
      let desc = typeof item.description === 'string' ? item.description : (item.description?._ || item.summary || item.content?._ || '');
      desc = desc.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
      if (desc.length > 300) desc = desc.substring(0, 300) + '...';
      let pubDate = item.pubDate || item.published || item.updated || '';
      return {
        id: `${source.name}_${idx}_${Date.now()}`,
        title: title.trim(),
        link: link.trim(),
        description: desc,
        pubDate: pubDate,
        source: source.name,
        lang: source.lang,
        region: source.region,
        weight: source.weight
      };
    }).filter(item => item.title && item.link && isWithin24Hours(item.pubDate));
  } catch (e) {
    console.error(`[ERROR] ${source.name}: ${e.message}`);
    return [];
  }
}

async function translateText(text, lang, retries = 2) {
  if (!text || text.length < 2) return '';
  const cacheKey = `${lang}_${text.substring(0, 100)}`;
  if (newsCache.translated[cacheKey]) return newsCache.translated[cacheKey];

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const result = await translate(text.substring(0, 500), {
        from: lang,
        to: TRANSLATE_TARGET,
        forceFrom: true,
        forceTo: true,
      });
      if (result.text) {
        newsCache.translated[cacheKey] = result.text;
        return result.text;
      }
    } catch (e) {
      if (attempt === retries) {
        console.warn(`[TRANSLATE ERROR] ${lang}: ${e.message}`);
      } else {
        await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
      }
    }
  }
  return '';
}

async function processTranslations(items) {
  const toTranslate = items.filter(item => item.lang !== 'zh');
  for (let i = 0; i < toTranslate.length; i += TRANSLATE_CONCURRENCY) {
    const batch = toTranslate.slice(i, i + TRANSLATE_CONCURRENCY);
    await Promise.all(batch.map(async (item) => {
      const translated = await translateText(item.title, item.lang);
      if (translated) item.translation = translated;
    }));
    if (i + TRANSLATE_CONCURRENCY < toTranslate.length) {
      await new Promise(r => setTimeout(r, TRANSLATE_BATCH_DELAY_MS));
    }
  }
}

async function updateNews() {
  if (newsCache.updating) return;
  newsCache.updating = true;
  console.log(`[${new Date().toISOString()}] 开始更新新闻...`);
  const collected = [];
  const batchSize = 5;
  for (let i = 0; i < RSS_SOURCES.length; i += batchSize) {
    const batch = RSS_SOURCES.slice(i, i + batchSize);
    const results = await Promise.all(batch.map(s => fetchRSS(s)));
    results.forEach(items => collected.push(...items));
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
    } catch(e) { return 0; }
  });
  console.log(`[TRANSLATE] 开始翻译前30条...`);
  await processTranslations(allNews.slice(0, 30));
  newsCache.data = allNews;
  newsCache.lastUpdate = new Date().toISOString();
  newsCache.updating = false;
  console.log(`[DONE] 共 ${allNews.length} 条相关新闻（过去24小时），更新完成`);
}

app.get('/api/news', (req, res) => {
  const { lang, limit = 100, offset = 0 } = req.query;
  let data = newsCache.data;
  if (lang && lang !== 'all') data = data.filter(n => n.lang === lang);
  const total = data.length;
  data = data.slice(parseInt(offset), parseInt(offset) + parseInt(limit));
  res.json({ success: true, total, lastUpdate: newsCache.lastUpdate, updating: newsCache.updating, data });
});

app.get('/api/refresh', async (req, res) => {
  if (newsCache.updating) return res.json({ success: false, message: '更新中，请稍候' });
  updateNews().catch(console.error);
  res.json({ success: true, message: '已触发更新' });
});

app.get('/api/sources', (req, res) => {
  res.json({ success: true, sources: RSS_SOURCES.map(s => ({ name: s.name, lang: s.lang, region: s.region })) });
});

app.get('/api/version', (req, res) => {
  res.json({
    success: true,
    version: '2.1.0',
    translator: 'google-translate-api-x',
    filters: { maxAgeHours: 24, israelJewishOnly: true },
  });
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

cron.schedule('*/5 * * * *', () => {
  console.log('[CRON] 触发定时更新');
  updateNews().catch(console.error);
});

updateNews().catch(console.error);

app.listen(PORT, () => {
  console.log(`✡ 犹太社区与以色列新闻聚合器`);
  console.log(`🌐 服务器运行在 http://localhost:${PORT}`);
  console.log(`📰 监控 ${RSS_SOURCES.length} 个新闻源`);
  console.log(`⏰ 每5分钟自动更新`);
});
