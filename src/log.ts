const formatField = ([key, value]: [string, string | number | undefined]) =>
  value === undefined ? "" : `${key}=${value}`;

export const logLine = (
  message: string,
  fields: Record<string, string | number | undefined> = {},
) => {
  const renderedFields = Object.entries(fields)
    .map(formatField)
    .filter(Boolean)
    .join(" ");

  const prefix = new Date().toISOString();
  const suffix = renderedFields ? ` ${renderedFields}` : "";
  console.error(`${prefix} ${message}${suffix}`);
};
