import { writeFileSync } from 'node:fs';
import { Canvas } from './png.js';

export interface HistogramOptions {
  title: string;
  subtitle?: string;
  xLabel: string;
  /** Samples are clipped at this value so one outlier cannot flatten the chart. */
  clipAtPercentile?: number;
  buckets?: number;
  width?: number;
  height?: number;
}

export interface Percentiles {
  p50: number;
  p90: number;
  p95: number;
  p99: number;
  p999: number;
  min: number;
  max: number;
  mean: number;
  count: number;
}

export function percentiles(samples: number[]): Percentiles {
  if (samples.length === 0) {
    return { p50: 0, p90: 0, p95: 0, p99: 0, p999: 0, min: 0, max: 0, mean: 0, count: 0 };
  }
  const sorted = [...samples].sort((a, b) => a - b);
  const at = (q: number): number => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] as number;
  return {
    p50: at(0.5),
    p90: at(0.9),
    p95: at(0.95),
    p99: at(0.99),
    p999: at(0.999),
    min: sorted[0] as number,
    max: sorted[sorted.length - 1] as number,
    mean: sorted.reduce((a, b) => a + b, 0) / sorted.length,
    count: sorted.length,
  };
}

const INK: [number, number, number] = [32, 38, 48];
const MUTED: [number, number, number] = [130, 138, 150];
const GRID: [number, number, number] = [228, 231, 236];
const BAR: [number, number, number] = [70, 120, 200];
const P99: [number, number, number] = [214, 79, 56];
const P50: [number, number, number] = [60, 160, 105];

/**
 * Renders a latency histogram to a PNG.
 *
 * The x axis is clipped at p99.9 by default. Latency distributions are heavily
 * right-skewed - one 4-second outlier among 100k sub-millisecond samples would
 * otherwise compress every real bar into the leftmost pixel column and make the
 * chart useless. The clipped tail is still reported numerically in the legend,
 * so nothing is hidden, only rescaled.
 */
export function renderHistogram(
  samples: number[],
  path: string,
  options: HistogramOptions,
): Percentiles {
  const width = options.width ?? 1000;
  const height = options.height ?? 520;
  const bucketCount = options.buckets ?? 60;
  const stats = percentiles(samples);

  const canvas = new Canvas(width, height, [255, 255, 255]);
  const left = 70;
  const right = width - 30;
  const top = 78;
  const bottom = height - 62;
  const plotWidth = right - left;
  const plotHeight = bottom - top;

  canvas.text(24, 24, options.title, INK, 2);
  if (options.subtitle !== undefined) canvas.text(24, 50, options.subtitle, MUTED, 1);

  const clipAt = options.clipAtPercentile ?? 0.999;
  const sorted = [...samples].sort((a, b) => a - b);
  const upper = Math.max(
    sorted[Math.min(sorted.length - 1, Math.floor(clipAt * sorted.length))] ?? 1,
    0.001,
  );
  const bucketWidth = upper / bucketCount;

  const counts = new Array<number>(bucketCount).fill(0);
  let clipped = 0;
  for (const sample of samples) {
    if (sample > upper) {
      clipped += 1;
      continue;
    }
    const index = Math.min(bucketCount - 1, Math.max(0, Math.floor(sample / bucketWidth)));
    counts[index] = (counts[index] ?? 0) + 1;
  }
  const peak = Math.max(1, ...counts);

  // Horizontal grid + y axis labels.
  for (let i = 0; i <= 4; i += 1) {
    const y = bottom - Math.round((i / 4) * plotHeight);
    canvas.hline(left, y, plotWidth, GRID);
    const label = formatCount(Math.round((i / 4) * peak));
    canvas.text(left - 8 - canvas.textWidth(label), y - 3, label, MUTED);
  }

  // Bars.
  const barWidth = plotWidth / bucketCount;
  for (let i = 0; i < bucketCount; i += 1) {
    const value = counts[i] ?? 0;
    const barHeight = Math.round((value / peak) * plotHeight);
    if (barHeight <= 0) continue;
    const x = left + Math.round(i * barWidth);
    const w = Math.max(1, Math.round(barWidth) - 1);
    canvas.fill(x, bottom - barHeight, w, barHeight, BAR);
  }

  // Axes.
  canvas.hline(left, bottom, plotWidth, INK);
  canvas.vline(left, top, plotHeight, INK);

  // X axis ticks.
  for (let i = 0; i <= 6; i += 1) {
    const x = left + Math.round((i / 6) * plotWidth);
    canvas.vline(x, bottom, 4, INK);
    const label = formatMs((i / 6) * upper);
    canvas.text(x - canvas.textWidth(label) / 2, bottom + 10, label, MUTED);
  }
  const xLabelX = left + plotWidth / 2 - canvas.textWidth(options.xLabel) / 2;
  canvas.text(xLabelX, bottom + 28, options.xLabel, INK);

  // Percentile markers.
  const marker = (value: number, colour: [number, number, number], label: string): void => {
    if (value > upper) return;
    const x = left + Math.round((value / upper) * plotWidth);
    canvas.vdashed(x, top, plotHeight, colour);
    canvas.text(Math.min(x + 4, right - canvas.textWidth(label)), top - 12, label, colour);
  };
  marker(stats.p50, P50, `P50 ${formatMs(stats.p50)}`);
  marker(stats.p99, P99, `P99 ${formatMs(stats.p99)}`);

  // Legend.
  const legend = [
    `SAMPLES ${formatCount(stats.count)}`,
    `MEAN ${formatMs(stats.mean)}`,
    `P50 ${formatMs(stats.p50)}`,
    `P90 ${formatMs(stats.p90)}`,
    `P99 ${formatMs(stats.p99)}`,
    `P99.9 ${formatMs(stats.p999)}`,
    `MAX ${formatMs(stats.max)}`,
  ];
  let legendX = left;
  for (const item of legend) {
    canvas.text(legendX, height - 22, item, MUTED);
    legendX += canvas.textWidth(item) + 18;
  }
  if (clipped > 0) {
    canvas.text(left, height - 34, `${clipped} SAMPLES ABOVE ${formatMs(upper)} NOT SHOWN`, MUTED);
  }

  writeFileSync(path, canvas.toPng());
  return stats;
}

function formatMs(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(1)}S`;
  if (ms >= 10) return `${ms.toFixed(0)}MS`;
  if (ms >= 1) return `${ms.toFixed(1)}MS`;
  return `${ms.toFixed(2)}MS`;
}

function formatCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1000) return `${(n / 1000).toFixed(1)}K`;
  return String(n);
}
