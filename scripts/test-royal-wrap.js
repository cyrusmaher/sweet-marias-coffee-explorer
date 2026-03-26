const assert = require("assert");
const path = require("path");
const { chromium } = require("playwright");
const { startStaticServer } = require("./serve-docs");

const TARGET_NAME = "Crown Jewel Colombia Carbonic Honey Peach Co-Ferment Edwin Noreña";
const DESKTOP_VIEWPORT = { width: 1504, height: 980 };
const LUNR_STUB = `
  window.lunr = function(builderFn) {
    const builder = {
      add() {},
      field() {},
      ref() {},
    };
    builderFn.call(builder);
    return {
      search() {
        return [];
      },
    };
  };
`;

async function main() {
  const repoRoot = path.resolve(__dirname, "..");
  const { server, url } = await startStaticServer({ rootDir: repoRoot, port: 0 });
  const browser = await chromium.launch({ headless: true });

  try {
    const page = await browser.newPage({ viewport: DESKTOP_VIEWPORT, deviceScaleFactor: 1 });

    await page.route("https://cdnjs.cloudflare.com/ajax/libs/lunr.js/2.3.9/lunr.min.js", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "text/javascript; charset=utf-8",
        body: LUNR_STUB,
      });
    });

    await page.goto(`${url}/docs/royal.html`, { waitUntil: "load" });
    await page.waitForFunction(() => document.querySelectorAll("#table-body tr").length > 0);

    const row = page.locator("#table-body tr").filter({ hasText: TARGET_NAME }).first();
    await row.scrollIntoViewIfNeeded();
    await row.click();
    await page.waitForSelector(".detail-row .detail-inner");

    const result = await page.evaluate(() => {
      const detail = document.querySelector(".detail-row .detail-inner");
      const specsGrid = detail.querySelector(".detail-specs");
      const specItems = Array.from(detail.querySelectorAll(".detail-specs .spec-item"));
      const specs = specItems.map((item) => {
        const label = item.querySelector(".spec-label");
        const value = item.querySelector(".spec-value");
        const rect = item.getBoundingClientRect();
        const valueRect = value.getBoundingClientRect();
        const computedValue = getComputedStyle(value);
        const lineHeight = parseFloat(computedValue.lineHeight) || 0;

        return {
          label: label.textContent.trim(),
          itemHeight: rect.height,
          itemWidth: rect.width,
          itemClientWidth: item.clientWidth,
          itemScrollWidth: item.scrollWidth,
          valueClientWidth: value.clientWidth,
          valueScrollWidth: value.scrollWidth,
          valueHeight: valueRect.height,
          wrapped: lineHeight > 0 ? valueRect.height > lineHeight * 1.5 : value.getClientRects().length > 1,
          overflow: item.scrollWidth > item.clientWidth + 1 || value.scrollWidth > value.clientWidth + 1,
          box: {
            top: rect.top,
            right: rect.right,
            bottom: rect.bottom,
            left: rect.left,
          },
        };
      });

      const overlaps = [];
      for (let index = 0; index < specs.length; index += 1) {
        for (let inner = index + 1; inner < specs.length; inner += 1) {
          const a = specs[index].box;
          const b = specs[inner].box;
          const intersects =
            a.left < b.right - 1 &&
            a.right > b.left + 1 &&
            a.top < b.bottom - 1 &&
            a.bottom > b.top + 1;
          if (intersects) {
            overlaps.push([specs[index].label, specs[inner].label]);
          }
        }
      }

      return {
        gridTemplateColumns: getComputedStyle(specsGrid).gridTemplateColumns,
        columnCount: (() => {
          const value = getComputedStyle(specsGrid).gridTemplateColumns;
          const matches = value.match(/(?:minmax|fit-content|calc)\([^)]*\)|[^\s]+/g);
          return matches ? matches.length : 0;
        })(),
        specs,
        overlaps,
      };
    });

    const wrappedLabels = result.specs.filter((spec) => spec.wrapped).map((spec) => spec.label);
    const overflowing = result.specs.filter((spec) => spec.overflow);
    const varietySpec = result.specs.find((spec) => spec.label === "Variety:");

    await page.locator(".detail-row .detail-inner").screenshot({
      path: "/tmp/green-coffee-explorer-royal-wrap-check.png",
    });

    assert(result.columnCount > 0, "Royal specs grid did not render");
    assert(result.columnCount <= 3, `Expected at most 3 desktop spec columns, got ${result.columnCount}`);
    assert.strictEqual(overflowing.length, 0, `Expected no overflowing spec items, got ${JSON.stringify(overflowing, null, 2)}`);
    assert.strictEqual(result.overlaps.length, 0, `Expected no overlapping spec items, got ${JSON.stringify(result.overlaps)}`);
    assert(varietySpec, "Expected a Variety spec for the Edwin Noreña peach row");
    assert(varietySpec.wrapped, "Expected the Variety value to wrap onto a new line at desktop width");

    console.log(JSON.stringify({
      checked: TARGET_NAME,
      viewport: DESKTOP_VIEWPORT,
      columnCount: result.columnCount,
      gridTemplateColumns: result.gridTemplateColumns,
      wrappedLabels,
      screenshot: "/tmp/green-coffee-explorer-royal-wrap-check.png",
    }, null, 2));
  } finally {
    await browser.close();
    await new Promise((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
