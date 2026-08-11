import { beforeEach, expect, it, vi } from "vitest";
import {
  cancelKleinJob,
  getKleinJob,
  getKleinJobs,
  rescaleKleinOperator,
} from "./klein";
import { get, post } from "./requestHandlers";

vi.mock("./requestHandlers", () => ({
  get: vi.fn(),
  post: vi.fn(),
}));

beforeEach(() => {
  vi.clearAllMocks();
});

it("uses the jobs collection endpoint", () => {
  getKleinJobs();
  expect(get).toHaveBeenCalledWith("api/klein/jobs");
});

it("encodes job identifiers in item actions", () => {
  getKleinJob("team/job 1");
  cancelKleinJob("team/job 1");
  expect(get).toHaveBeenCalledWith("api/klein/jobs/team%2Fjob%201");
  expect(post).toHaveBeenCalledWith("api/klein/jobs/team%2Fjob%201/cancel");
});

it("sends the typed rescale payload", () => {
  rescaleKleinOperator("job 1", 7, 4);
  expect(post).toHaveBeenCalledWith(
    "api/klein/jobs/job%201/operators/7/rescale",
    { parallelism: 4 },
  );
});
