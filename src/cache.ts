import { LocalDest, type Dest } from "@subsquid/file-store";
import { S3Dest } from "@subsquid/file-store-s3";
import { createReadStream } from "node:fs";
import { readdir } from "node:fs/promises";
import { join } from "node:path";
import {
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { normalizeAddress } from "./format.js";
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
  const accessKeyId = assertEnv(
    env.CACHE_S3_ACCESS_KEY_ID || env.AWS_ACCESS_KEY_ID,
    "CACHE_S3_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID",
  );
  const secretAccessKey = assertEnv(
    env.CACHE_S3_SECRET_ACCESS_KEY || env.AWS_SECRET_ACCESS_KEY,
    "CACHE_S3_SECRET_ACCESS_KEY or AWS_SECRET_ACCESS_KEY",
  );
  const sessionToken = env.CACHE_S3_SESSION_TOKEN || env.AWS_SESSION_TOKEN;
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

const createS3ClientConfigFromEnv = (env: NodeJS.ProcessEnv) => {
  const endpointValue = assertEnv(env.CACHE_S3_ENDPOINT, "CACHE_S3_ENDPOINT");
  const region = assertEnv(env.CACHE_S3_REGION, "CACHE_S3_REGION");
  const accessKeyId = assertEnv(
    env.CACHE_S3_ACCESS_KEY_ID || env.AWS_ACCESS_KEY_ID,
    "CACHE_S3_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID",
  );
  const secretAccessKey = assertEnv(
    env.CACHE_S3_SECRET_ACCESS_KEY || env.AWS_SECRET_ACCESS_KEY,
    "CACHE_S3_SECRET_ACCESS_KEY or AWS_SECRET_ACCESS_KEY",
  );
  const sessionToken = env.CACHE_S3_SESSION_TOKEN || env.AWS_SESSION_TOKEN;
  const endpoint = /^[a-z]+:\/\//i.test(endpointValue) ? endpointValue : `https://${endpointValue}`;
  const forcePathStyle =
    env.CACHE_S3_FORCE_PATH_STYLE ? env.CACHE_S3_FORCE_PATH_STYLE !== "false" : true;

  return {
    endpoint,
    region,
    forcePathStyle,
    credentials: {
      accessKeyId,
      secretAccessKey,
      ...(sessionToken ? { sessionToken } : {}),
    },
  };
};

export const getCacheDest = (chain: Chain, env: NodeJS.ProcessEnv = process.env) =>
  env.CACHE_DEST ? withNetworkSuffix(env.CACHE_DEST, chain) : `./cache/${normalizeNetwork(chain)}`;

export const isS3Uri = (value: string) => value.startsWith("s3://");

export const createFileStoreDest = (
  cacheUri: string,
  env: NodeJS.ProcessEnv = process.env,
): Dest =>
  isS3Uri(cacheUri)
    ? new S3Dest(cacheUri, createS3ClientConfigFromEnv(env))
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
