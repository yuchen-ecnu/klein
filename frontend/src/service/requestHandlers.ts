const formatUrl = (url: string) => (url.startsWith("/") ? url.slice(1) : url);

export type RequestConfig = Omit<RequestInit, "body" | "method"> & {
  timeout?: number;
};

export type APIResponse<T> = {
  data: T;
  headers: Headers;
  status: number;
};

export class APIRequestError<T = unknown> extends Error {
  readonly response: APIResponse<T>;

  constructor(response: APIResponse<T>) {
    super(`Request failed with status ${response.status}`);
    this.name = "APIRequestError";
    this.response = response;
  }
}

export const isAPIRequestError = <T = unknown>(
  error: unknown,
): error is APIRequestError<T> => error instanceof APIRequestError;

const request = async <T>(
  method: "GET" | "POST",
  url: string,
  data: unknown,
  config: RequestConfig = {},
): Promise<APIResponse<T>> => {
  const { timeout, ...requestConfig } = config;
  const controller = timeout === undefined ? undefined : new AbortController();
  const timer =
    timeout === undefined
      ? undefined
      : window.setTimeout(() => controller?.abort(), timeout);
  const headers = new Headers(requestConfig.headers);
  if (data !== undefined && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  try {
    const response = await fetch(formatUrl(url), {
      ...requestConfig,
      body: data === undefined ? undefined : JSON.stringify(data),
      headers,
      method,
      signal: controller?.signal ?? requestConfig.signal,
    });
    const responseData = (response.status === 204
      ? undefined
      : response.headers.get("Content-Type")?.includes("application/json")
        ? await response.json()
        : await response.text()) as T;
    const result = {
      data: responseData,
      headers: response.headers,
      status: response.status,
    };
    if (!response.ok) {
      throw new APIRequestError(result);
    }
    return result;
  } finally {
    if (timer !== undefined) {
      window.clearTimeout(timer);
    }
  }
};

export const get = <T = unknown>(
  url: string,
  config?: RequestConfig,
): Promise<APIResponse<T>> => request<T>("GET", url, undefined, config);

export const post = <T = unknown>(
  url: string,
  data?: unknown,
  config?: RequestConfig,
): Promise<APIResponse<T>> => request<T>("POST", url, data, config);
