const DEFAULT_TTL_MS = 60 * 60 * 1000;

function normalizeCurrency(c) {
  return String(c || "").trim().toUpperCase();
}

function buildCacheKey(from, to) {
  return `${from}->${to}`;
}

async function getLatestRateFromDb({ supabase, base, quote }) {
  const { data, error } = await supabase
    .from("fx_rates")
    .select("rate, fetched_at")
    .eq("base_currency", base)
    .eq("quote_currency", quote)
    .order("fetched_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) return null;
  if (!data) return null;
  const rate = Number(data.rate);
  if (!Number.isFinite(rate) || rate <= 0) return null;
  return { rate, fetched_at: data.fetched_at };
}

async function upsertRatesToDb({ supabase, provider, fetchedAt, rates }) {
  const rows = Object.entries(rates).map(([pair, rate]) => {
    const [base, quote] = pair.split("->");
    return {
      base_currency: base,
      quote_currency: quote,
      rate,
      provider,
      fetched_at: fetchedAt,
    };
  });

  if (rows.length === 0) return;
  await supabase.from("fx_rates").insert(rows);
}

async function fetchEurBaseRates({ fetchImpl }) {
  const url = "https://api.frankfurter.app/latest?from=EUR&to=USD,CAD";
  const res = await fetchImpl(url);
  if (!res.ok) throw new Error(`fx_provider_http_${res.status}`);
  const json = await res.json();

  const usd = Number(json?.rates?.USD);
  const cad = Number(json?.rates?.CAD);
  if (!Number.isFinite(usd) || usd <= 0) throw new Error("fx_invalid_rate_usd");
  if (!Number.isFinite(cad) || cad <= 0) throw new Error("fx_invalid_rate_cad");

  return {
    provider: "frankfurter",
    fetchedAt: new Date().toISOString(),
    eurUsd: usd,
    eurCad: cad,
  };
}

export function createFxService({ supabase, ttlMs = DEFAULT_TTL_MS, fetchImpl = globalThis.fetch } = {}) {
  if (!supabase) throw new Error("fx_supabase_required");
  if (!fetchImpl) throw new Error("fx_fetch_required");

  let lastRefreshAt = 0;
  const cache = new Map();

  async function refreshCacheIfNeeded() {
    const now = Date.now();
    if (now - lastRefreshAt < ttlMs && cache.size > 0) return;

    try {
      const r = await fetchEurBaseRates({ fetchImpl });
      const eurUsd = r.eurUsd;
      const eurCad = r.eurCad;

      const rates = {
        [buildCacheKey("EUR", "USD")]: eurUsd,
        [buildCacheKey("EUR", "CAD")]: eurCad,
        [buildCacheKey("USD", "EUR")]: 1 / eurUsd,
        [buildCacheKey("CAD", "EUR")]: 1 / eurCad,
        [buildCacheKey("USD", "CAD")]: eurCad / eurUsd,
        [buildCacheKey("CAD", "USD")]: eurUsd / eurCad,
      };

      cache.clear();
      for (const [k, v] of Object.entries(rates)) cache.set(k, v);
      lastRefreshAt = now;

      await upsertRatesToDb({
        supabase,
        provider: r.provider,
        fetchedAt: r.fetchedAt,
        rates,
      });
    } catch {
      lastRefreshAt = now;
    }
  }

  async function getRate(fromCurrency, toCurrency) {
    const from = normalizeCurrency(fromCurrency);
    const to = normalizeCurrency(toCurrency);
    if (!from || !to) throw new Error("fx_currency_required");
    if (from === to) return 1;

    await refreshCacheIfNeeded();

    const key = buildCacheKey(from, to);
    const cached = cache.get(key);
    if (Number.isFinite(cached) && cached > 0) return cached;

    const db = await getLatestRateFromDb({ supabase, base: from, quote: to });
    if (db?.rate) return db.rate;

    throw new Error("fx_rate_unavailable");
  }

  async function convertMinorAmount({ amount, fromCurrency, toCurrency }) {
    if (!Number.isFinite(amount)) throw new Error("fx_amount_invalid");
    const rate = await getRate(fromCurrency, toCurrency);
    const converted = Math.round(Number(amount) * rate);
    return { amount: converted, rate };
  }

  return {
    getRate,
    convertMinorAmount,
  };
}
