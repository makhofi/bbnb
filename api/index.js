import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

export const config = {
  api: { bodyParser: false },
  supportsResponseStreaming: true,
  maxDuration: 60,
};

const BACKEND_HOST = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");

const FILTERED_HEADERS = new Set([
  "host",
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
]);

export default async function handler(incomingReq, outgoingRes) {
  if (!BACKEND_HOST) {
    outgoingRes.statusCode = 500;
    return outgoingRes.end("Missing configuration: TARGET_DOMAIN env var required");
  }

  try {
    const destination = BACKEND_HOST + incomingReq.url;

    const forwardHeaders = {};
    let originalIp = null;
    
    for (const key of Object.keys(incomingReq.headers)) {
      const lowerKey = key.toLowerCase();
      const headerValue = incomingReq.headers[key];
      
      if (FILTERED_HEADERS.has(lowerKey)) continue;
      if (lowerKey.startsWith("x-vercel-")) continue;
      
      if (lowerKey === "x-real-ip") {
        originalIp = headerValue;
        continue;
      }
      
      if (lowerKey === "x-forwarded-for") {
        if (!originalIp) originalIp = headerValue;
        continue;
      }
      
      forwardHeaders[lowerKey] = Array.isArray(headerValue) ? headerValue.join(", ") : headerValue;
    }
    
    if (originalIp) forwardHeaders["x-forwarded-for"] = originalIp;

    const httpMethod = incomingReq.method;
    const supportsBody = httpMethod !== "GET" && httpMethod !== "HEAD";

    const fetchOptions = { method: httpMethod, headers: forwardHeaders, redirect: "manual" };
    
    if (supportsBody) {
      fetchOptions.body = Readable.toWeb(incomingReq);
      fetchOptions.duplex = "half";
    }

    const backendResponse = await fetch(destination, fetchOptions);

    outgoingRes.statusCode = backendResponse.status;
    
    for (const [headerKey, headerVal] of backendResponse.headers) {
      if (headerKey.toLowerCase() === "transfer-encoding") continue;
      try {
        outgoingRes.setHeader(headerKey, headerVal);
      } catch {}
    }

    if (backendResponse.body) {
      await pipeline(Readable.fromWeb(backendResponse.body), outgoingRes);
    } else {
      outgoingRes.end();
    }
  } catch (error) {
    console.error("Relay error:", error);
    if (!outgoingRes.headersSent) {
      outgoingRes.statusCode = 502;
      outgoingRes.end("Gateway Error: Unable to reach upstream");
    }
  }
}