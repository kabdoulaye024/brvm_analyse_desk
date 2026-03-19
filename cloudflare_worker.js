/**
 * BRVM Proxy Worker
 * Relays GET requests to any target URL, bypassing Streamlit Cloud IP blocks.
 * Deploy at: https://workers.cloudflare.com/
 *
 * Usage: https://your-worker.your-subdomain.workers.dev/?url=<encoded-target-url>
 *
 * Security: only allows BRVM-related domains.
 */

const ALLOWED_DOMAINS = [
  "richbourse.com",
  "brvm.org",
  "sikafinance.com",
  "madisinvest.com",
];

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  Accept:
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
  "Accept-Encoding": "gzip, deflate, br",
  Connection: "keep-alive",
  "Cache-Control": "max-age=0",
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const targetUrl = url.searchParams.get("url");

    if (!targetUrl) {
      return new Response("Missing ?url= parameter", { status: 400 });
    }

    // Validate allowed domains
    let targetHost;
    try {
      targetHost = new URL(targetUrl).hostname;
    } catch {
      return new Response("Invalid target URL", { status: 400 });
    }

    const allowed = ALLOWED_DOMAINS.some((d) => targetHost.endsWith(d));
    if (!allowed) {
      return new Response(`Domain not allowed: ${targetHost}`, { status: 403 });
    }

    try {
      const response = await fetch(targetUrl, {
        method: "GET",
        headers: HEADERS,
        redirect: "follow",
      });

      // Forward response with CORS headers so Streamlit can read it
      const newHeaders = new Headers(response.headers);
      newHeaders.set("Access-Control-Allow-Origin", "*");
      newHeaders.set("Cache-Control", "no-store");

      return new Response(response.body, {
        status: response.status,
        headers: newHeaders,
      });
    } catch (err) {
      return new Response(`Fetch error: ${err.message}`, { status: 502 });
    }
  },
};
