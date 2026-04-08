import { LocalDest, type Dest } from "@subsquid/file-store";
import { createReadStream } from "node:fs";
import { readdir } from "node:fs/promises";
import { join, posix as pathPosix } from "node:path";
import {
  DeleteObjectsCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { normalizeAddress } from "./format.js";
import { logLine } from "./log.js";
import type { Chain, TransferChunk } from "./types.js";

const normalizeNetwork = (network: string) => network.trim().toLowerCase();

const splitPath = (value: string) =>
  value
    .split(/[\\/]/)
    .map((part) => part.trim())
    .filter(Boolean);

const withNetworkSuffix = (dest: string, chain: Chain) => {
  const network = normalizeNetwork(chain);

  if (dest.startsWith("s3://")) {
    const url = new URL(dest);
    const parts = splitPath(url.pathname);
    const hasNetwork = parts.at(-1)?.toLowerCase() === network;
    const nextPath = hasNetwork ? parts : [...parts, network];
    return `s3://${url.hostname}/${nextPath.join("/")}`.replace(/\/+$/, "");
  }

  const parts = splitPath(dest);
  const hasNetwork = parts.at(-1)?.toLowerCase() === network;
  return hasNetwork ? dest.replace(/[\\/]+$/, "") : `${dest.replace(/[\\/]+$/, "")}/${network}`;
};

const assertEnv = (value: string | undefined, key: string) => {
  if (!value) {
    throw new Error(`${key} is required when CACHE_DEST points at s3://...`);
  }

  return value;
};

const parseS3Uri = (value: string) => {
  const url = new URL(value);

  if (url.protocol !== "s3:") {
    throw new Error(`expected s3:// cache URI, got ${value}`);
  }

  return {
    bucket: url.hostname,
    key: url.pathname.replace(/^\/+/, ""),
  };
};

const parseTransferChunk = (value: string) => {
  const range = value.match(/(?:^|\/)(\d+)-(\d+)(?:\/[^/]+\.parquet|\.parquet)$/);
  const token = value.match(/token=(0x[a-f0-9]{40})/i);
  const tokenAddress = token?.[1];
  const fromBlock = range?.[1];
  const toBlock = range?.[2];

  if (!fromBlock || !toBlock) {
    return null;
  }

  return {
    cacheUri: value,
    ...(tokenAddress ? { tokenAddress: normalizeAddress(tokenAddress) } : {}),
    fromBlock: Number(fromBlock),
    toBlock: Number(toBlock),
  } satisfies TransferChunk;
};

const overlapsRange = (
  chunk: Pick<TransferChunk, "fromBlock" | "toBlock">,
  fromBlock?: number,
  toBlock?: number,
) =>
  (fromBlock === undefined || chunk.toBlock >= fromBlock) &&
  (toBlock === undefined || chunk.fromBlock <= toBlock);

const sortTransferChunks = (chunks: TransferChunk[]) =>
  [...chunks].sort(
    (left, right) =>
      String(left.tokenAddress ?? "").localeCompare(String(right.tokenAddress ?? "")) ||
      left.fromBlock - right.fromBlock ||
      right.toBlock - left.toBlock ||
      left.cacheUri.localeCompare(right.cacheUri),
  );

const createS3ClientFromEnv = (env: NodeJS.ProcessEnv) => {
  const endpointValue = assertEnv(env.CACHE_S3_ENDPOINT, "CACHE_S3_ENDPOINT");
  const region = assertEnv(env.CACHE_S3_REGION, "CACHE_S3_REGION");
  const accessKeyId = assertEnv(env.CACHE_S3_ACCESS_KEY_ID, "CACHE_S3_ACCESS_KEY_ID");
  const secretAccessKey = assertEnv(env.CACHE_S3_SECRET_ACCESS_KEY, "CACHE_S3_SECRET_ACCESS_KEY");
  const sessionToken = env.CACHE_S3_SESSION_TOKEN;
  const endpoint = /^[a-z]+:\/\//i.test(endpointValue) ? endpointValue : `https://${endpointValue}`;
  const forcePathStyle =
    env.CACHE_S3_FORCE_PATH_STYLE ? env.CACHE_S3_FORCE_PATH_STYLE !== "false" : true;

  return new S3Client({
    endpoint,
    region,
    forcePathStyle,
    credentials: {
      accessKeyId,
      secretAccessKey,
      ...(sessionToken ? { sessionToken } : {}),
    },
  });
};

const isDir = (value: string) => value.endsWith("/");
const toDir = (value: string) => (isDir(value) ? value : `${value}/`);
const toS3Url = (bucket: string, dir: string) => new URL(dir, `s3://${bucket}`).toString();
const normalizeErrorMessage = (value: unknown) =>
  value instanceof Error ? value.message : typeof value === "string" ? value : "unknown error";

const isAwsError = (value: unknown): value is {
  name?: string;
  message?: string;
  Code?: string;
  $metadata?: { httpStatusCode?: number };
} => typeof value === "object" && value !== null;

const createS3Error = (
  operation: string,
  context: Record<string, string | number | undefined>,
  error: unknown,
) => {
  const fields = Object.entries(context)
    .filter(([, value]) => value !== undefined)
    .map(([key, value]) => `${key}=${value}`)
    .join(" ");
  const status = isAwsError(error) ? error.$metadata?.httpStatusCode : undefined;
  const code = isAwsError(error) ? error.Code ?? error.name : undefined;
  const message = normalizeErrorMessage(error);
  return new Error(
    [`s3 ${operation} failed`, fields, status ? `status=${status}` : "", code ? `code=${code}` : "", `error=${message}`]
      .filter(Boolean)
      .join(" "),
    { cause: error instanceof Error ? error : undefined },
  );
};

const withS3Context = async <T>(
  operation: string,
  context: Record<string, string | number | undefined>,
  thunk: () => Promise<T>,
) => {
  try {
    return await thunk();
  } catch (error) {
    throw createS3Error(operation, context, error);
  }
};

class CompatibleS3Dest implements Dest {
  private bucket: string;
  private dir: string;

  constructor(cacheUri: string, private client: S3Client) {
    const { bucket, key } = parseS3Uri(cacheUri);
    this.bucket = bucket;
    this.dir = `/${key.replace(/^\/+/, "")}`;
  }

  path(...paths: string[]) {
    return pathPosix.join("/", this.dir, ...paths);
  }

  private key(...paths: string[]) {
    const key = this.path(...paths);
    return key.startsWith("/") ? key.slice(1) : key;
  }

  private async listKeys(prefix: string, delimiter?: string, maxKeys?: number) {
    return withS3Context(
      "list",
      {
        bucket: this.bucket,
        prefix,
        delimiter,
        max_keys: maxKeys,
      },
      () =>
        this.client.send(
          new ListObjectsV2Command({
            Bucket: this.bucket,
            Prefix: prefix,
            ...(delimiter ? { Delimiter: delimiter } : {}),
            ...(maxKeys ? { MaxKeys: maxKeys } : {}),
          }),
        ),
    );
  }

  private async existsFile(name: string) {
    const key = this.key(name);
    const response = await this.listKeys(key, undefined, 1);

    return (response.Contents ?? []).some((item) => item.Key === key);
  }

  private async existsDir(name: string) {
    const prefix = this.key(toDir(name));
    const response = await this.listKeys(prefix, undefined, 1);
    return (response.Contents ?? []).length > 0;
  }

  async exists(name: string) {
    if (isDir(name)) {
      return this.existsDir(name);
    }

    return (await this.existsFile(name)) || this.existsDir(name);
  }

  async readFile(name: string) {
    const key = this.key(name);
    const response = await withS3Context(
      "read",
      { bucket: this.bucket, key },
      () =>
        this.client.send(
          new GetObjectCommand({
            Bucket: this.bucket,
            Key: key,
          }),
        ),
    );

    if (!response.Body) {
      throw new Error(`missing S3 body for ${name}`);
    }

    return response.Body.transformToString("utf-8");
  }

  async writeFile(name: string, data: string | Uint8Array) {
    const key = this.key(name);
    await withS3Context(
      "write",
      { bucket: this.bucket, key },
      () =>
        this.client.send(
          new PutObjectCommand({
            Bucket: this.bucket,
            Key: key,
            Body: typeof data === "string" ? Buffer.from(data, "utf8") : data,
          }),
        ),
    );
  }

  async mkdir(name: string) {
    const key = this.key(toDir(name));
    await withS3Context(
      "mkdir",
      { bucket: this.bucket, key },
      () =>
        this.client.send(
          new PutObjectCommand({
            Bucket: this.bucket,
            Key: key,
            Body: new Uint8Array(),
          }),
        ),
    );
  }

  async readdir(name: string) {
    const dir = toDir(name);

    if (!(await this.exists(dir))) {
      return [];
    }

    const prefix = this.key(dir);
    const names = new Set<string>();
    let continuationToken: string | undefined;

    do {
      const response = await withS3Context(
        "list",
        {
          bucket: this.bucket,
          prefix,
          delimiter: "/",
          continuation: continuationToken,
        },
        () =>
          this.client.send(
            new ListObjectsV2Command({
              Bucket: this.bucket,
              Prefix: prefix,
              Delimiter: "/",
              ContinuationToken: continuationToken,
            }),
          ),
      );

      (response.CommonPrefixes ?? []).forEach((item) => {
        const value = item.Prefix;

        if (!value) {
          return;
        }

        names.add(value.slice(prefix.length, value.length - 1));
      });

      (response.Contents ?? []).forEach((item) => {
        const value = item.Key;

        if (!value || value === prefix) {
          return;
        }

        names.add(value.slice(prefix.length));
      });

      continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
    } while (continuationToken);

    return [...names].sort();
  }

  async rm(name: string) {
    if (isDir(name) || !(await this.existsFile(name))) {
      await this.rmDir(name);
      return;
    }

    await withS3Context(
      "delete",
      { bucket: this.bucket, key: this.key(name), objects: 1 },
      () =>
        this.client.send(
          new DeleteObjectsCommand({
            Bucket: this.bucket,
            Delete: {
              Objects: [{ Key: this.key(name) }],
              Quiet: true,
            },
          }),
        ),
    );
  }

  private async rmDir(name: string) {
    const prefix = this.key(toDir(name));
    let continuationToken: string | undefined;

    do {
      const response = await withS3Context(
        "list",
        {
          bucket: this.bucket,
          prefix,
          continuation: continuationToken,
        },
        () =>
          this.client.send(
            new ListObjectsV2Command({
              Bucket: this.bucket,
              Prefix: prefix,
              ContinuationToken: continuationToken,
            }),
          ),
      );

      const objects = (response.Contents ?? [])
        .map((item) => item.Key)
        .filter((key): key is string => typeof key === "string")
        .map((key) => ({ Key: key }));

      if (objects.length > 0) {
        await withS3Context(
          "delete",
          {
            bucket: this.bucket,
            prefix,
            objects: objects.length,
          },
          () =>
            this.client.send(
              new DeleteObjectsCommand({
                Bucket: this.bucket,
                Delete: {
                  Objects: objects,
                  Quiet: true,
                },
              }),
            ),
        );
      }

      continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
    } while (continuationToken);
  }

  async transact(name: string, cb: (txDest: Dest) => Promise<void>) {
    const txDest = new CompatibleS3Dest(toS3Url(this.bucket, this.path(name)), this.client);
    await cb(txDest);
  }
}

export const getCacheDest = (chain: Chain, env: NodeJS.ProcessEnv = process.env) =>
  env.CACHE_DEST ? withNetworkSuffix(env.CACHE_DEST, chain) : `./cache/${normalizeNetwork(chain)}`;

export const isS3Uri = (value: string) => value.startsWith("s3://");

export const createFileStoreDest = (
  cacheUri: string,
  env: NodeJS.ProcessEnv = process.env,
): Dest =>
  isS3Uri(cacheUri)
    ? new CompatibleS3Dest(cacheUri, createS3ClientFromEnv(env))
    : new LocalDest(cacheUri);

export const getTokenManifestPath = ({ cacheDest }: { cacheDest: string }) =>
  [cacheDest.replace(/\/+$/, ""), "token-manifest.parquet"].join("/");

export const uploadFileToCache = async ({
  localPath,
  cacheUri,
  env = process.env,
}: {
  localPath: string;
  cacheUri: string;
  env?: NodeJS.ProcessEnv;
}) => {
  if (!isS3Uri(cacheUri)) {
    throw new Error(`uploadFileToCache requires an s3:// URI, got ${cacheUri}`);
  }

  const { bucket, key } = parseS3Uri(cacheUri);
  const client = createS3ClientFromEnv(env);

  await client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: createReadStream(localPath),
      ContentType: "application/octet-stream",
    }),
  );
};

const listLocalFiles = async (directory: string): Promise<string[]> => {
  try {
    const entries = await readdir(directory, { withFileTypes: true });
    const nested = await Promise.all(
      entries.map((entry) =>
        entry.isDirectory()
          ? listLocalFiles(join(directory, entry.name))
          : Promise.resolve([join(directory, entry.name)]),
      ),
    );
    return nested.flat();
  } catch (error) {
    const code =
      error && typeof error === "object" && "code" in error ? String(error.code) : undefined;

    if (code === "ENOENT") {
      return [];
    }

    throw error;
  }
};

const listS3Keys = async ({
  bucket,
  prefix,
  env,
}: {
  bucket: string;
  prefix: string;
  env: NodeJS.ProcessEnv;
}) => {
  const client = createS3ClientFromEnv(env);
  const keys: string[] = [];
  let continuationToken: string | undefined;

  do {
    const response = await client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      }),
    );

    keys.push(
      ...(response.Contents ?? [])
        .map((item) => item.Key)
        .filter((key): key is string => typeof key === "string"),
    );
    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return keys;
};

const getS3Prefix = ({ cacheDest }: { cacheDest: string }) => {
  const { key } = parseS3Uri(cacheDest);
  const prefix = key.replace(/\/+$/, "");

  return [prefix, "erc20-transfers"].join("/") + "/";
};

export const listTransferChunks = async ({
  cacheDest,
  tokenAddress,
  fromBlock,
  toBlock,
  env = process.env,
}: {
  cacheDest: string;
  tokenAddress?: string;
  fromBlock?: number;
  toBlock?: number;
  env?: NodeJS.ProcessEnv;
}) => {
  const cacheUris = isS3Uri(cacheDest)
    ? await (async () => {
        const { bucket } = parseS3Uri(cacheDest);
        const prefix = getS3Prefix({ cacheDest });
        const keys = await listS3Keys({ bucket, prefix, env });
        return keys.map((key) => `s3://${bucket}/${key}`);
      })()
    : await listLocalFiles([cacheDest.replace(/[\\/]+$/, ""), "erc20-transfers"].join("/"));

  return sortTransferChunks(
    cacheUris
      .map(parseTransferChunk)
      .filter((chunk): chunk is TransferChunk => chunk !== null)
      .filter(
        (chunk) => tokenAddress === undefined || chunk.tokenAddress === undefined || chunk.tokenAddress === tokenAddress,
      )
      .filter((chunk) => overlapsRange(chunk, fromBlock, toBlock)),
  );
};

export const probeFileStoreDest = async ({
  chain,
  env = process.env,
}: {
  chain: Chain;
  env?: NodeJS.ProcessEnv;
}) => {
  const cacheDest = getCacheDest(chain, env);
  const transferDest = `${cacheDest.replace(/\/+$/, "")}/erc20-transfers`;
  const dest = createFileStoreDest(transferDest, env);
  const probeName = `_probe-${Date.now()}.txt`;

  logLine("probing file-store dest", { chain, dest: transferDest });
  const statusExists = await dest.exists("status.txt");
  logLine("probed file-store status", { chain, exists: Number(statusExists) });

  if (statusExists) {
    const status = await dest.readFile("status.txt");
    logLine("read file-store status", {
      chain,
      status: JSON.stringify(status.trim()),
    });
  }

  const names = await dest.readdir("./");
  logLine("listed file-store root", { chain, entries: names.length });
  await dest.writeFile(probeName, "probe");
  logLine("wrote file-store probe", { chain, file: probeName });
  await dest.readFile(probeName);
  logLine("read file-store probe", { chain, file: probeName });
  await dest.rm(probeName);
  logLine("removed file-store probe", { chain, file: probeName });
};
