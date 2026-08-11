import axios from "axios";
import { beforeEach, expect, it, vi } from "vitest";
import { get, post } from "./requestHandlers";

vi.mock("axios", () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
  },
}));

beforeEach(() => {
  vi.clearAllMocks();
});

it("normalizes a leading slash for GET requests", () => {
  const config = { timeout: 100 };
  get("/api/jobs", config);
  expect(axios.get).toHaveBeenCalledWith("api/jobs", config);
});

it("preserves relative POST paths and payloads", () => {
  const payload = { parallelism: 3 };
  post("api/jobs/1", payload);
  expect(axios.post).toHaveBeenCalledWith("api/jobs/1", payload, undefined);
});
