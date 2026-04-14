type DuneUploadColumn = {
  name: string;
  type: string;
  nullable?: boolean;
};

type DuneUploadedTable = {
  namespace: string;
  table_name: string;
  full_name: string;
};

type DuneCreateUploadResponse = DuneUploadedTable & {
  already_existed?: boolean;
  message?: string;
  example_query?: string;
};

type DuneListUploadsResponse = {
  next_offset?: number | null;
  tables?: DuneUploadedTable[];
};

type DuneInsertResponse = {
  bytes_written: number;
  name: string;
  rows_written: number;
};

type DuneClearResponse = {
  name: string;
  rows_cleared: number;
};

const requireApiKey = (value: string | undefined) => {
  if (!value) {
    throw new Error("missing DUNE_API_KEY");
  }

  return value;
};

const parseJson = <T>(value: string) => (value.trim().length === 0 ? ({} as T) : (JSON.parse(value) as T));

const fetchJson = async <T>(input: string, init?: RequestInit) => {
  const response = await fetch(input, init);
  const payloadText = await response.text();
  const payload = parseJson<T & { error?: string; message?: string }>(payloadText);

  if (!response.ok) {
    throw new Error(`Dune uploads API error: ${response.status} ${JSON.stringify(payload)}`);
  }

  return payload as T;
};

const duneHeaders = (apiKey: string, contentType?: string) => ({
  "X-DUNE-API-KEY": apiKey,
  ...(contentType ? { "Content-Type": contentType } : {}),
});

export const listUploadedTables = async (apiKey = process.env.DUNE_API_KEY) => {
  const duneApiKey = requireApiKey(apiKey);
  const tables: DuneUploadedTable[] = [];
  let offset = 0;

  for (;;) {
    const response = await fetchJson<DuneListUploadsResponse>(
      `https://api.dune.com/api/v1/uploads?limit=100&offset=${offset}`,
      { headers: duneHeaders(duneApiKey) },
    );
    tables.push(...(response.tables || []));

    if (response.next_offset === undefined || response.next_offset === null) {
      return tables;
    }

    offset = response.next_offset;
  }
};

export const ensureUploadedTable = async ({
  namespace,
  tableName,
  description,
  schema,
  apiKey = process.env.DUNE_API_KEY,
}: {
  namespace: string;
  tableName: string;
  description: string;
  schema: DuneUploadColumn[];
  apiKey?: string;
}) => {
  const duneApiKey = requireApiKey(apiKey);
  const existing = (await listUploadedTables(duneApiKey)).find(
    (table) => table.namespace === namespace && table.table_name === tableName,
  );

  if (existing) {
    return existing;
  }

  return fetchJson<DuneCreateUploadResponse>("https://api.dune.com/api/v1/uploads", {
    method: "POST",
    headers: duneHeaders(duneApiKey, "application/json"),
    body: JSON.stringify({
      namespace,
      table_name: tableName,
      description,
      is_private: false,
      schema,
    }),
  });
};

export const clearUploadedTable = async ({
  namespace,
  tableName,
  apiKey = process.env.DUNE_API_KEY,
}: {
  namespace: string;
  tableName: string;
  apiKey?: string;
}) =>
  fetchJson<DuneClearResponse>(`https://api.dune.com/api/v1/uploads/${namespace}/${tableName}/clear`, {
    method: "POST",
    headers: duneHeaders(requireApiKey(apiKey), "application/json"),
    body: JSON.stringify({}),
  });

export const insertUploadedCsv = async ({
  namespace,
  tableName,
  csv,
  apiKey = process.env.DUNE_API_KEY,
}: {
  namespace: string;
  tableName: string;
  csv: string;
  apiKey?: string;
}) =>
  fetchJson<DuneInsertResponse>(`https://api.dune.com/api/v1/uploads/${namespace}/${tableName}/insert`, {
    method: "POST",
    headers: duneHeaders(requireApiKey(apiKey), "text/csv"),
    body: csv,
  });

export const normalizeUploadedTableName = (value: string) => {
  const normalized = value.trim();

  if (!/^dune\.[a-z0-9_]+\.[a-z0-9_]+$/i.test(normalized)) {
    throw new Error(`unexpected uploaded Dune table name: ${value}`);
  }

  return normalized;
};
