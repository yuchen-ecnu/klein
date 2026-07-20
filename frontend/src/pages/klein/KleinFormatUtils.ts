const formatNumber = (value: number) =>
  value.toLocaleString(undefined, { maximumFractionDigits: 1 });

export const formatCount = (value: number) => {
  const count = value ?? 0;
  const absolute = Math.abs(count);
  if (absolute >= 1_000_000_000) {
    return `${formatNumber(count / 1_000_000_000)}B`;
  }
  if (absolute >= 1_000_000) {
    return `${formatNumber(count / 1_000_000)}M`;
  }
  if (absolute >= 1_000) {
    return `${formatNumber(count / 1_000)}K`;
  }
  return formatNumber(count);
};

export const formatRate = (value: number) => formatCount(value);

export const formatBytes = (bytes: number) => {
  const value = bytes ?? 0;
  if (value < 1024) {
    return `${formatNumber(value)} B`;
  }
  if (value < 1024 ** 2) {
    return `${formatNumber(value / 1024)} KB`;
  }
  if (value < 1024 ** 3) {
    return `${formatNumber(value / 1024 ** 2)} MB`;
  }
  if (value < 1024 ** 4) {
    return `${formatNumber(value / 1024 ** 3)} GB`;
  }
  return `${formatNumber(value / 1024 ** 4)} TB`;
};

export const formatByteRate = (bytes: number) => `${formatBytes(bytes)}/s`;
