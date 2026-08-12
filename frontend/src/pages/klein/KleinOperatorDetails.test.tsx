import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, expect, it, vi } from "vitest";
import { operatorFixture, subtaskFixture } from "../../test/fixtures";
import { KleinOperatorDetails } from "./KleinOperatorDetails";

vi.mock("./KleinOperatorRescaleControl", () => ({
  KleinOperatorRescaleControl: () => <div>Rescale control</div>,
}));

afterEach(cleanup);

const renderDetails = (
  operator: Parameters<typeof KleinOperatorDetails>[0]["operator"],
  inDrawer = false,
) =>
  render(
    <MemoryRouter>
      <KleinOperatorDetails
        inDrawer={inDrawer}
        jobId="job-1"
        onRefresh={vi.fn()}
        operator={operator}
      />
    </MemoryRouter>,
  );

it("prompts for a selection when no operator is provided", () => {
  renderDetails(undefined);

  expect(
    screen.getByText(/Select an operator in the DAG or table/),
  ).toBeInTheDocument();
});

it("renders operator metrics, task rows, and Ray actor links", () => {
  renderDetails(
    operatorFixture({
      parallelism: 1,
      subtasks: [
        subtaskFixture({
          actor_id: "actor-1",
          backpressure_percent: 60,
          busy_percent: 75,
        }),
      ],
    }),
  );

  expect(screen.getByRole("heading", { name: "map" })).toBeInTheDocument();
  expect(screen.getByText("Operator 1 · 1 task instance · 1 CPU / 0 GPU each")).toBeInTheDocument();
  expect(screen.getAllByRole("link", { name: "View actor" })).toHaveLength(2);
  for (const link of screen.getAllByRole("link", { name: "View actor" })) {
    expect(link).toHaveAttribute("href", "/actors/actor-1");
  }
  expect(screen.getByText("#0")).toBeInTheDocument();
  expect(screen.getByText("75.0%")).toBeInTheDocument();
  expect(screen.getByText("60.0% · 1 events")).toBeInTheDocument();
});

it("renders compact drawer task metrics and unavailable actor state", () => {
  renderDetails(
    operatorFixture({
      subtasks: [subtaskFixture({ actor_id: null })],
    }),
    true,
  );

  expect(screen.getByText("Actor unavailable")).toBeDisabled();
  expect(screen.getByText("Busy")).toBeInTheDocument();
  expect(screen.getByText("BP")).toBeInTheDocument();
  expect(screen.getByText("25.0%")).toBeInTheDocument();
  expect(screen.getByText("5.0%")).toBeInTheDocument();
});

it("renders the empty task-metrics state", () => {
  renderDetails(operatorFixture({ subtasks: [] }));

  expect(screen.getByText("Task metrics are not available yet.")).toBeInTheDocument();
});
