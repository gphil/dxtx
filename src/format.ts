export const normalizeAddress = (value: string) => {
  const trimmed = value.trim().toLowerCase();
  const isAddress = /^0x[a-f0-9]{40}$/.test(trimmed);

  if (!isAddress) {
    throw new Error(`invalid address: ${value}`);
  }

  return trimmed;
};

export const bigIntHexToDecimal = (hexValue: string) =>
  BigInt(hexValue === "0x" ? "0x0" : hexValue).toString(10);

export const formatUnitsText = (amountRaw: string, decimals: number) => {
  const sign = amountRaw.startsWith("-") ? "-" : "";
  const digits = sign ? amountRaw.slice(1) : amountRaw;

  if (decimals === 0) {
    return `${sign}${digits}`;
  }

  const padded = digits.padStart(decimals + 1, "0");
  const split = padded.length - decimals;
  const whole = padded.slice(0, split);
  const fraction = padded.slice(split).replace(/0+$/, "");

  return fraction ? `${sign}${whole}.${fraction}` : `${sign}${whole}`;
};

export const safeNumberFromText = (value: string) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

export const escapeSqlString = (value: string) => value.replaceAll("'", "''");
