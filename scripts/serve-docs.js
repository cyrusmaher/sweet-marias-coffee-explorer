const fs = require("fs");
const path = require("path");
const http = require("http");

const MIME_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".ico": "image/x-icon",
  ".jpg": "image/jpeg",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".txt": "text/plain; charset=utf-8",
  ".webp": "image/webp",
};

function getContentType(filePath) {
  return MIME_TYPES[path.extname(filePath).toLowerCase()] || "application/octet-stream";
}

function resolveRequestPath(rootDir, requestUrl) {
  const requestPath = decodeURIComponent(new URL(requestUrl, "http://127.0.0.1").pathname);
  const normalizedPath = requestPath.replace(/^\/+/, "");
  let filePath = path.resolve(rootDir, normalizedPath);

  if (!filePath.startsWith(path.resolve(rootDir))) {
    return null;
  }

  if (fs.existsSync(filePath) && fs.statSync(filePath).isDirectory()) {
    filePath = path.join(filePath, "index.html");
  }

  return filePath;
}

function createStaticServer({ rootDir }) {
  return http.createServer((req, res) => {
    const filePath = resolveRequestPath(rootDir, req.url || "/");

    if (!filePath) {
      res.writeHead(403, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("Forbidden");
      return;
    }

    fs.readFile(filePath, (error, data) => {
      if (error) {
        const status = error.code === "ENOENT" ? 404 : 500;
        res.writeHead(status, { "Content-Type": "text/plain; charset=utf-8" });
        res.end(status === 404 ? "Not found" : "Internal server error");
        return;
      }

      res.writeHead(200, { "Content-Type": getContentType(filePath) });
      res.end(data);
    });
  });
}

function startStaticServer({
  rootDir = path.resolve(__dirname, ".."),
  host = "127.0.0.1",
  port = 4173,
} = {}) {
  return new Promise((resolve, reject) => {
    const server = createStaticServer({ rootDir });
    server.once("error", reject);
    server.listen(port, host, () => {
      const address = server.address();
      const url = `http://${host}:${address.port}`;
      resolve({ server, url });
    });
  });
}

async function main() {
  const portArg = process.argv[2];
  const parsedPort = portArg ? Number(portArg) : 4173;
  const { url } = await startStaticServer({
    port: Number.isFinite(parsedPort) ? parsedPort : 4173,
  });

  console.log(`Serving green-coffee-explorer at ${url}`);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

module.exports = {
  startStaticServer,
};
