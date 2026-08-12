import { act, renderHook } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";
import { rescaleKleinOperator } from "../../../service/klein";
import { useKleinOperatorRescale } from "./useKleinJobs";

vi.mock("../../../service/klein", () => ({
  cancelKleinJob: vi.fn(),
  getKleinJob: vi.fn(),
  getKleinJobs: vi.fn(),
  rescaleKleinOperator: vi.fn(),
}));

beforeEach(() => vi.clearAllMocks());

it("publishes a successful rescale result and refreshes the job", async () => {
  const refresh = vi.fn().mockResolvedValue(undefined);
  const result = {
    operation_id: "resize-1",
    job_id: "job-1",
    operator_id: 1,
    previous_parallelism: 2,
    parallelism: 4,
    target_parallelism: 4,
    status: "COMPLETED" as const,
    started_at_ms: 1,
    ended_at_ms: 2,
  };
  vi.mocked(rescaleKleinOperator).mockResolvedValue({ data: result } as never);
  const { result: hook } = renderHook(() => useKleinOperatorRescale("job-1", refresh));

  await act(async () => {
    expect(await hook.current.rescale(1, 4)).toEqual(result);
  });

  expect(rescaleKleinOperator).toHaveBeenCalledWith("job-1", 1, 4);
  expect(refresh).toHaveBeenCalledOnce();
  expect(hook.current.result).toEqual(result);
  expect(hook.current.isRescaling).toBe(false);
});

it("polls while an accepted rescale is non-terminal", async () => {
  vi.useFakeTimers();
  const refresh = vi.fn().mockResolvedValue(undefined);
  const accepted = {
    operation_id: "resize-1",
    job_id: "job-1",
    operator_id: 1,
    previous_parallelism: 2,
    parallelism: 4,
    target_parallelism: 4,
    status: "ACCEPTED" as const,
    phase: "QUEUED" as const,
  };
  vi.mocked(rescaleKleinOperator).mockResolvedValue({ data: accepted } as never);
  const { result: hook, unmount } = renderHook(() =>
    useKleinOperatorRescale("job-1", refresh),
  );

  await act(async () => {
    await hook.current.rescale(1, 4);
  });
  expect(hook.current.operationPending).toBe(true);
  expect(refresh).toHaveBeenCalledTimes(1);

  await act(async () => {
    await vi.advanceTimersByTimeAsync(1_000);
  });
  expect(refresh).toHaveBeenCalledTimes(2);
  unmount();
  vi.useRealTimers();
});

it("adopts and polls an active operation from the refreshed snapshot", async () => {
  vi.useFakeTimers();
  const refresh = vi.fn().mockResolvedValue(undefined);
  const running = {
    operation_id: "resize-external",
    job_id: "job-1",
    operator_id: 1,
    previous_parallelism: 2,
    parallelism: 4,
    target_parallelism: 4,
    status: "RUNNING" as const,
    phase: "COORDINATING" as const,
  };
  const { result: hook, unmount } = renderHook(() =>
    useKleinOperatorRescale("job-1", refresh, running),
  );

  expect(hook.current.result).toEqual(running);
  expect(hook.current.operationPending).toBe(true);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(1_000);
  });
  expect(refresh).toHaveBeenCalledOnce();
  unmount();
  vi.useRealTimers();
});

it("rejects a missing job ID before issuing a request", async () => {
  const refresh = vi.fn();
  const { result: hook } = renderHook(() => useKleinOperatorRescale(undefined, refresh));

  await act(async () => {
    expect(await hook.current.rescale(1, 4)).toBeUndefined();
  });

  expect(hook.current.requestError).toBe("A Klein job ID is required.");
  expect(rescaleKleinOperator).not.toHaveBeenCalled();
  expect(refresh).not.toHaveBeenCalled();
});
