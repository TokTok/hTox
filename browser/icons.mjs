// Writes hTox's Android vector drawables as SVG, for Hatter's browser host
// (res/<name>.svg): node icons.mjs <drawable dir> <out dir>. Tinted ones take
// the colour of the text around them, as Android tints them with the theme's.

import { mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";

const [from, to] = process.argv.slice(2);
mkdirSync(to, { recursive: true });

const attribute = (element, name) => element.match(new RegExp(`android:${name}="([^"]*)"`))?.[1];

// Android's #AARRGGBB (or #RRGGBB), as CSS.
const color = (value) => {
  if (!value || !value.startsWith("#")) return "currentColor";
  return value.length === 9 ? `#${value.slice(3)}${value.slice(1, 3)}` : value;
};

for (const file of readdirSync(from).filter((f) => f.endsWith(".xml"))) {
  const xml = readFileSync(join(from, file), "utf8");
  const vector = xml.match(/<vector[^>]*>/)?.[0];
  if (!vector) continue;
  const tinted = attribute(vector, "tint") !== undefined;
  const paint = (value) => (tinted ? "currentColor" : color(value));
  const paths = [...xml.matchAll(/<path[^>]*>/g)].map(([path]) => {
    const fill = attribute(path, "fillColor");
    const stroke = attribute(path, "strokeColor");
    return `<path d="${attribute(path, "pathData")}" fill="${fill ? paint(fill) : "none"}"` +
      (stroke ? ` stroke="${paint(stroke)}" stroke-width="${attribute(path, "strokeWidth") ?? 1}"` +
        ` stroke-linecap="${attribute(path, "strokeLineCap") ?? "butt"}" stroke-linejoin="${attribute(path, "strokeLineJoin") ?? "miter"}"` : "") +
      "/>";
  });
  const size = (name) => parseFloat(attribute(vector, name));
  writeFileSync(join(to, `${basename(file, ".xml")}.svg`),
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${size("viewportWidth")} ${size("viewportHeight")}" ` +
    `width="${size("width")}" height="${size("height")}">${paths.join("")}</svg>\n`);
}
