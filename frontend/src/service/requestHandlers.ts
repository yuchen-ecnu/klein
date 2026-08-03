import axios, { type AxiosPromise, type AxiosRequestConfig } from "axios";

const formatUrl = (url: string) => (url.startsWith("/") ? url.slice(1) : url);

export const get = <T = unknown>(
  url: string,
  config?: AxiosRequestConfig,
): AxiosPromise<T> => axios.get<T>(formatUrl(url), config);

export const post = <T = unknown>(
  url: string,
  data?: unknown,
  config?: AxiosRequestConfig,
): AxiosPromise<T> => axios.post<T>(formatUrl(url), data, config);
