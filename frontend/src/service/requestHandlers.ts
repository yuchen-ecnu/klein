import axios, { AxiosRequestConfig, AxiosResponse } from "axios";

const formatUrl = (url: string) => (url.startsWith("/") ? url.slice(1) : url);

export const get = <T = unknown, R = AxiosResponse<T>>(
  url: string,
  config?: AxiosRequestConfig,
): Promise<R> => axios.get<T, R>(formatUrl(url), config);

export const post = <T = unknown, R = AxiosResponse<T>>(
  url: string,
  data?: unknown,
  config?: AxiosRequestConfig,
): Promise<R> => axios.post<T, R>(formatUrl(url), data, config);
