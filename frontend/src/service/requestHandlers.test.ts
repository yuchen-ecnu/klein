import { beforeEach, expect, it, vi } from "vitest";
import { get, isAPIRequestError, post } from "./requestHandlers";

beforeEach(() => {
  vi.clearAllMocks();
  vi.stubGlobal("fetch", vi.fn());
});

it("normalizes a leading slash for GET requests", async () => {
  vi.mocked(fetch).mockResolvedValue(
    new Response('{"jobs":[]}', {
      headers: { "Content-Type": "application/json" },
    }),
  );

  await expect(get<{ jobs: unknown[] }>("/api/jobs")).resolves.toMatchObject({
    data: { jobs: [] },
    status: 200,
  });
  expect(fetch).toHaveBeenCalledWith(
    "api/jobs",
    expect.objectContaining({ method: "GET" }),
  );
});

it("preserves relative POST paths and JSON payloads", async () => {
  vi.mocked(fetch).mockResolvedValue(
    new Response('{"accepted":true}', {
      headers: { "Content-Type": "application/json" },
    }),
  );
  const payload = { parallelism: 3 };
  await post("api/jobs/1", payload);

  const [, init] = vi.mocked(fetch).mock.calls[0];
  expect(fetch).toHaveBeenCalledWith(
    "api/jobs/1",
    expect.objectContaining({ body: JSON.stringify(payload), method: "POST" }),
  );
  expect(new Headers(init?.headers).get("Content-Type")).toBe(
    "application/json",
  );
});

it("exposes structured JSON errors", async () => {
  vi.mocked(fetch).mockResolvedValue(
    new Response('{"error":"busy"}', {
      headers: { "Content-Type": "application/json" },
      status: 409,
    }),
  );

  const error = await post("api/jobs/1", {}).catch((caught) => caught);

  expect(isAPIRequestError<{ error: string }>(error)).toBe(true);
  if (isAPIRequestError<{ error: string }>(error)) {
    expect(error.response.data.error).toBe("busy");
    expect(error.response.status).toBe(409);
  }
});
