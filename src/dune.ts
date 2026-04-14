type DuneExecuteResponse = {
  execution_id: string;
  state: string;
};

type DuneStatusResponse = {
  state: string;
  is_execution_finished: boolean;
  error?: unknown;
};

type DuneResultsResponse<T> = {
  next_offset?: number;
  result?: {
    rows: T[];
  };
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const requireApiKey = (value: string | undefined) => {
  if (!value) {
    throw new Error("missing DUNE_API_KEY");
  }

  return value;
};

const duneHeaders = (apiKey: string, includeContentType = false) => ({
  ...(includeContentType ? { "Content-Type": "application/json" } : {}),
  "X-DUNE-API-KEY": apiKey,
});

const parseJson = <T>(value: string) => {
  if (value.trim().length === 0) {
    return {} as T;
  }

  return JSON.parse(value) as T;
};

const fetchJson = async <T>(input: string, init?: RequestInit, attempt = 0): Promise<T> => {
  const response = await fetch(input, init);
  const payloadText = await response.text();
  const payload = parseJson<T>(payloadText);

  if (response.status === 429 && attempt < 5) {
    await sleep((attempt + 1) * 2_000);
    return fetchJson<T>(input, init, attempt + 1);
  }

  if (!response.ok) {
    throw new Error(`Dune API error: ${response.status} ${JSON.stringify(payload)}`);
  }

  return payload;
};

const isCompletedState = (state: string) => state.includes("COMPLETED");

const isFailedState = (state: string) =>
  state.includes("FAILED") || state.includes("CANCELLED") || state.includes("EXPIRED");

export const executeDuneSql = async <T>({
  sql,
  apiKey = process.env.DUNE_API_KEY,
  pageSize = 10_000,
  pollMs = 1_000,
  maxPolls = 120,
}: {
  sql: string;
  apiKey?: string;
  pageSize?: number;
  pollMs?: number;
  maxPolls?: number;
}) => {
  const duneApiKey = requireApiKey(apiKey);
  const execution = await fetchJson<DuneExecuteResponse>("https://api.dune.com/api/v1/sql/execute", {
    method: "POST",
    headers: duneHeaders(duneApiKey, true),
    body: JSON.stringify({
      sql,
      performance: "medium",
    }),
  });

  for (let attempt = 0; attempt < maxPolls; attempt += 1) {
    const status = await fetchJson<DuneStatusResponse>(
      `https://api.dune.com/api/v1/execution/${execution.execution_id}/status`,
      {
        headers: duneHeaders(duneApiKey),
      },
    );

    if (status.is_execution_finished && isCompletedState(status.state)) {
      const rows: T[] = [];
      let offset = 0;

      for (;;) {
        const result = await fetchJson<DuneResultsResponse<T>>(
          `https://api.dune.com/api/v1/execution/${execution.execution_id}/results?limit=${pageSize}&offset=${offset}`,
          {
            headers: duneHeaders(duneApiKey),
          },
        );

        rows.push(...(result.result?.rows ?? []));

        if (result.next_offset === undefined || result.next_offset === null) {
          return rows;
        }

        offset = result.next_offset;
      }
    }

    if (status.is_execution_finished && isFailedState(status.state)) {
      throw new Error(`Dune execution failed: ${status.state} ${JSON.stringify(status)}`);
    }

    await sleep(pollMs);
  }

  throw new Error(`timed out waiting for Dune execution ${execution.execution_id}`);
};
