const express = require('express');
const fetch = require('node-fetch');
const xml2js = require('xml2js');
const cron = require('node-cron');
const cors = require('cors');
const path = require('path');

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

const TRANSLATE_API = 'https://api.mymemory.translated.net/get';
const CACHE_DURATION = 5 * 60 * 1000;

let newsCache = { data: [], translated: {}, lastUpdate: null, updating: false };

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
    return items.slice(0, 15).map((item, idx) => {
      let title = typeof item.title === 'string' ? item.title : (item.title?._ || '');
      let link = typeof item.link === 'string' ? item.link : (item.link?.href || '');
      if (Array.isArray(item.link)) link = item.link[0]?.href || item.link[0] || '';
      let desc = typeof item.description === 'string' ? item.description : (item.description?._ || item.summary || item.content?._ || '');
      desc = desc.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
      if (desc.length > 300) desc = desc.substring(0, 300) + '...';
      let pubDate = item.pubDate || item.published || item.updated || new Date().toISOString();
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
    }).filter(item => item.title && item.link);
  } catch (e) {
    console.error(`[ERROR] ${source.name}: ${e.message}`);
    return [];
  }
}

async function translateText(text, lang) {
  if (!text || text.length < 2) return '';
  const cacheKey = `${lang}_${text.substring(0, 100)}`;
  if (newsCache.translated[cacheKey]) return newsCache.translated[cacheKey];
  try {
    const pair = lang === 'he' ? 'iw|zh-CN' : `${lang}|zh-CN`;
    const url = `${TRANSLATE_API}?q=${encodeURIComponent(text.substring(0, 500))}&langpair=${pair}`;
    const res = await fetch(url);
    const data = await res.json();
    if (data.responseData?.translatedText) {
      newsCache.translated[cacheKey] = data.responseData.translatedText;
      return data.responseData.translatedText;
    }
  } catch (e) { console.warn(`[TRANSLATE ERROR] ${e.message}`); }
  return '';
}

async function processTranslations(items) {
  for (const item of items) {
    if (item.lang === 'zh') continue;
    const translated = await translateText(item.title, item.lang);
    if (translated) item.translation = translated;
    await new Promise(r => setTimeout(r, 200));
  }
}

async function updateNews() {
  if (newsCache.updating) return;
  newsCache.updating = true;
  console.log(`[${new Date().toISOString()}] 开始更新新闻...`);
  const allNews = [];
  const batchSize = 5;
  for (let i = 0; i < RSS_SOURCES.length; i += batchSize) {
    const batch = RSS_SOURCES.slice(i, i + batchSize);
    const results = await Promise.all(batch.map(s => fetchRSS(s)));
    results.forEach(items => allNews.push(...items));
    console.log(`[PROGRESS] 已获取 ${allNews.length} 条`);
  }
  allNews.sort((a, b) => {
    try {
      const timeDiff = new Date(b.pubDate) - new Date(a.pubDate);
      if (Math.abs(timeDiff) < 3600000) return (b.weight || 0) - (a.weight || 0);
      return timeDiff;
    } catch(e) { return 0; }
  });
  console.log(`[TRANSLATE] 开始翻译前20条...`);
  await processTranslations(allNews.slice(0, 20));
  newsCache.data = allNews;
  newsCache.lastUpdate = new Date().toISOString();
  newsCache.updating = false;
  console.log(`[DONE] 共 ${allNews.length} 条新闻，更新完成`);
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
