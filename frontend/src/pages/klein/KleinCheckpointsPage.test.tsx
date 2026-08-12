import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router";
import { beforeEach, expect, it, vi } from "vitest";
import { jobFixture } from "../../test/fixtures";
import { KleinCheckpointsPage } from "./KleinCheckpointsPage";

const mocks = vi.hoisted(() => ({ useKleinJob: vi.fn() }));

vi.mock("./hook/useKleinJobs", () => ({ useKleinJob: mocks.useKleinJob }));

beforeEach(() => mocks.useKleinJob.mockReset());

it("sorts and expands checkpoint operator details", async () => {
  const user = userEvent.setup();
  const job = jobFixture();
  job.checkpoints.history = [
    {
      id: 7,
      status: "COMPLETED",
      triggered_at_ms: 1_700_000_001_000,
      duration_ms: 1200,
      acknowledged: 2,
      required_acknowledgements: 2,
      state_size_bytes: 1024,
      alignment_duration_ms: 3,
      barrier_latency_ms: 4,
      operators: [
        {
          op_id: 1,
          name: "map",
          state_size_bytes: 1024,
          alignment_duration_ms: 3,
          barrier_latency_ms: 4,
          subtasks: [
            {
              subtask_index: 0,
              state_size_bytes: 1024,
              alignment_duration_ms: 3,
              barrier_latency_ms: 4,
              rows_in: 10,
              rows_out: 9,
            },
          ],
        },
      ],
    },
  ];
  mocks.useKleinJob.mockReturnValue({ job, error: undefined, isLoading: false });

  render(
    <MemoryRouter>
      <KleinCheckpointsPage />
    </MemoryRouter>,
  );

  expect(screen.getByText("file:///checkpoints/chk-7")).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "ID" }));
  expect(screen.getByText("Operator checkpoint details")).toBeInTheDocument();
  expect(screen.getByText(/#0: 1 KB/)).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Collapse checkpoint 7" }));
  await user.click(await screen.findByRole("button", { name: "Expand checkpoint 7" }));
  expect(await screen.findByText("Operator checkpoint details")).toBeInTheDocument();
});

it("renders an empty history and request failures", () => {
  mocks.useKleinJob.mockReturnValue({ job: jobFixture(), error: undefined, isLoading: false });
  const { rerender } = render(
    <MemoryRouter>
      <KleinCheckpointsPage />
    </MemoryRouter>,
  );
  expect(screen.getByText("No checkpoints recorded.")).toBeInTheDocument();

  mocks.useKleinJob.mockReturnValue({ job: undefined, error: new Error("offline"), isLoading: false });
  rerender(
    <MemoryRouter>
      <KleinCheckpointsPage />
    </MemoryRouter>,
  );
  expect(screen.getByText("Unable to load checkpoints.")).toBeInTheDocument();
});
